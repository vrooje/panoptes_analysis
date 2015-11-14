import sys

# file with raw classifications (csv)
# put this way up here so if there are no inputs we exit quickly before even trying to load everything else
default_statstart = "data_out/session_stats"
try:
    classfile_in = sys.argv[1]
except:
    #classfile_in = 'data/2e3d12a2-56ca-4d1f-930a-9ecc7fd39885.csv'
    print "\nUsage: "+sys.argv[0]+" classifications_infile [stats_outfile session_break_length]"
    print "      classifications_infile is a Zooniverse (Panoptes) classifications data export CSV."
    print "      stats_outfile is the name of an outfile you'd like to write."
    print "           if you don't specify one it will be "+default_statstart+"_[date]_to_[date].csv"
    print "           where the dates show the first & last classification date."
    print "      A new session is defined to start when 2 classifications by the same classifier are"
    print "           separated by at least session_break_length minutes (default value: 60)"
    print "\nOnly the classifications_infile is a required input.\n"
    sys.exit(0)



import numpy as np
import pandas as pd
#import matplotlib.pyplot as plt
import datetime
import dateutil.parser
#import csv
import json
#import mysql.connector
#import pymysql

from ast import literal_eval  # but for json don't use this, use json.loads




# timestamps & timediffs are in nanoseconds below but we want outputs in HOURS
# Note: I'd like to keep units in days but then a session length etc in seconds is ~1e-5 and that's too
#       close to floating-point errors for my liking (because this might be read into Excel)
# we will use either this below, or
# /datetime.timedelta(hours=1)
# depending on whether the output is in a timedelta (use above) or in float (use below).
ns2hours = 1.0 / (1.0e9*60.*60.)
ns2mins  = 1.0 / (1.0e9*60.)



# columns currently in an exported Panoptes classification file: 
# user_name,user_id,user_ip,workflow_id,workflow_name,workflow_version,created_at,gold_standard,expert,metadata,annotations,subject_data

# user_name is either their registered name or "not-logged-in"+their hashed IP
# user_id is their numeric Zooniverse ID or blank if they're unregistered
# user_ip is a hashed version of their IP
# workflow_id is the numeric ID of this workflow, which you can find in the project builder URL for managing the workflow:
#       https://www.zooniverse.org/lab/[project_id]/workflow/[workflow_id]/
# workflow_name is the name you gave your workflow (for sanity checks)
# workflow_version is [bigchangecount].[smallchangecount] and is probably pretty big
# created_at is the date the entry for the classification was recorded
# gold_standard is 1 if this classification was done in gold standard mode
# expert is 1 if this classification was done in expert mode... I think
# metadata (json) is the data the browser sent along with the classification. 
#       Includes browser information, language, started_at and finished_at
#       note started_at and finished_at are perhaps the easiest way to calculate the length of a classification
#       (the duration elapsed between consecutive created_at by the same user is another way)
#       the difference here is back-end vs front-end
# annotations (json) contains the actual classification information
#       which for this analysis we will ignore completely, for now
# subject_data is cross-matched from the subjects table and is for convenience in data reduction
#       here we will ignore this too
# we'll also ignore user_ip, workflow information, gold_standard, and expert.
#cols_used = ["user_name", "user_id", "created_at", "metadata"]
cols_used = ["created_at_ts", "user_name", "user_id", "created_at", "started_at", "finished_at"]



try:
    statsfile_out = sys.argv[2]
    modstatsfile = False
except:
    statsfile_out = default_statstart+".csv"
    modstatsfile = True


try:
    session_break = float(sys.argv[3]) #in minutes
except:
    session_break = 60.
    
    
print "Computing session stats using:"
print "   infile:",classfile_in
print "   new session starts after classifier break of",session_break,"minutes\n"






def sessionstats(grp):
    
    # groups and dataframes behave a bit differently; life is a bit easier if we DF the group
    # also sort each individual group rather than sort the whole classification dataframe; should be much faster
    user_class = pd.DataFrame(grp).sort('created_at_ts', ascending=True)
    
    try:
        theuserid = int(user_class.user_id.iloc[0])
    except:
        theuserid = user_class.user_id.iloc[0]
    
    user_class['duration'] = user_class.created_at_ts.diff()
    user_class['class_length'] = user_class.finished_at - user_class.started_at
    user_class['session'] = [1 for q in user_class.duration]
    user_class['count'] = [1 for q in user_class.duration] # because aggregate('count') has a weird bug
    user_class['created_day'] = [q[:10] for q in user_class.created_at]

    n_class    = len(user_class)    
    n_days     = len(user_class.created_day.unique())
    first_day  = user_class.created_day.iloc[0]
    last_day   = user_class.created_day.iloc[-1]

    #front-end version
    tdiff_firstlast_hours = (user_class.finished_at[user_class.index[-1]] - user_class.started_at[user_class.index[0]]).total_seconds() / 3600.
    
    
    i_firstclass = user_class.index[0]  
    i_lastclass  = user_class.index[-1]  

    # Figure out where new sessions start, manually dealing with the first classification of the session
    thefirst = (user_class.duration >= np.timedelta64(int(session_break), 'm')) | (user_class.index == i_firstclass)
    
    insession = np.invert(thefirst)
    starttimes = user_class.created_at_ts[thefirst]
    n_sessions = len(starttimes.unique())

    # timedeltas are just ints, but interpreted a certain way. So force them to int as needed
    class_length_mean_overall   = np.mean(user_class.class_length).astype(int) * ns2mins
    class_length_median_overall = np.median(user_class.class_length).astype(int) * ns2mins
    
    
    # index this into a timeseries
    # this means the index might no longer be unique, but it has many advantages
    user_class.set_index('created_at_ts', inplace=True, drop=False)
    
    
    user_class.session = user_class.session * 0
    # now, keep the session count by adding 1 to each element of the timeseries with t > each start time
    for the_start in starttimes.unique():
        user_class.session[the_start:] += 1
    
    
    # Now that we've defined the sessions let's do some calculations
    bysession = user_class.groupby('session')
    
    # get classification counts, total session durations, median classification length for each session
    # time units in minutes here
    # this will give a warning for 1-entry sessions but whatevs, let NaNs be NaNs
    class_length_median = bysession.class_length.apply(lambda x: np.median(x))/datetime.timedelta(minutes=1)
    class_length_total  = bysession.class_length.aggregate('sum') * ns2mins
    class_count_session = bysession.count.aggregate('sum')
    class_count_session_list = str(class_count_session.tolist()).replace(',',';')
#     # ignore the first duration, which isn't a real classification duration but a time between sessions
#     dur_median = bysession.duration.apply(lambda x: np.median(x[1:])) /datetime.timedelta(hours=1)
#     dur_total = bysession.duration.apply(lambda x: np.sum(x[1:]))  # in nanoseconds
#     ses_count = bysession.duration.aggregate('count')
# #    ses_nproj = bysession.project_name.aggregate(lambda x:x.nunique())
    
    count_mean = np.nanmean(class_count_session.astype(float))
    count_med  = np.median(class_count_session)
    count_min  = np.min(class_count_session)
    count_max  = np.max(class_count_session)
    
    session_length_mean    = np.nanmean(class_length_total).astype(float)
    session_length_median  = np.median(class_length_total).astype(float)
    session_length_min     = np.min(class_length_total)
    session_length_max     = np.max(class_length_total)
    session_length_total = np.sum(class_length_total)
     
    class_length_mean  = class_length_total / class_count_session.astype(float)
    
#     nproj_session_med  = np.median(ses_nproj)
#     nproj_session_mean = np.nanmean(ses_nproj.astype(float))
#     nproj_session_min  = np.min(ses_nproj)
#     nproj_session_max  = np.max(ses_nproj)
    
    
    if n_sessions >= 4:
        # get durations of first 2 and last 2 sessions
        mean_duration_first2 = (class_length_total[1]+class_length_total[2])/2.0
        mean_duration_last2  = (class_length_total[n_sessions]+class_length_total[n_sessions-1])/2.0
        mean_class_duration_first2 = (class_length_total[1]+class_length_total[2])/(class_count_session[1]+class_count_session[2]).astype(float)
        mean_class_duration_last2  = (class_length_total[n_sessions]+class_length_total[n_sessions-1])/(class_count_session[n_sessions]+class_count_session[n_sessions-1]).astype(float)
    else:
        mean_duration_first2 = 0.0
        mean_duration_last2  = 0.0
        mean_class_duration_first2 = 0.0
        mean_class_duration_last2  = 0.0
    
    
    # now set up the DF to return
    session_stats = {}
    session_stats["user_id"]                              = theuserid
    session_stats["n_class"]                              = n_class
    session_stats["n_sessions"]                           = n_sessions
    session_stats["n_days"]                               = n_days
    session_stats["first_day"]                            = first_day
    session_stats["last_day"]                             = last_day
    session_stats["tdiff_firstlast_hours"]                = tdiff_firstlast_hours
    session_stats["time_spent_classifying_total_minutes"] = session_length_total
    session_stats["class_per_session_min"]                = count_min
    session_stats["class_per_session_max"]                = count_max
    session_stats["class_per_session_med"]                = count_med
    session_stats["class_per_session_mean"]               = count_mean
    session_stats["class_length_mean_overall"]            = float(class_length_mean_overall)
    session_stats["class_length_median_overall"]          = class_length_median_overall
    session_stats["session_length_mean"]                  = session_length_mean
    session_stats["session_length_median"]                = session_length_median
    session_stats["session_length_min"]                   = session_length_min
    session_stats["session_length_max"]                   = session_length_max
    session_stats["mean_session_length_first2"]           = mean_duration_first2
    session_stats["mean_session_length_last2"]            = mean_duration_last2
    session_stats["mean_class_length_first2"]             = mean_class_duration_first2
    session_stats["mean_class_length_last2"]              = mean_class_duration_last2    
    session_stats["class_count_session_list"]             = class_count_session_list


    col_order = ['user_id',
            'n_class',
            'n_sessions',
            'n_days',
            'first_day',
            'last_day',
            'tdiff_firstlast_hours',
            'time_spent_classifying_total_minutes',
            'class_per_session_min',
            'class_per_session_max',
            'class_per_session_med',
            'class_per_session_mean',
            'class_length_mean_overall',
            'class_length_median_overall',
            'session_length_mean',
            'session_length_median',
            'session_length_min',
            'session_length_max',
            'mean_session_length_first2',
            'mean_session_length_last2',
            'mean_class_length_first2',
            'mean_class_length_last2',
            'class_count_session_list']


    return pd.Series(session_stats)[col_order]







# Begin the main stuff


print "Reading classifications from "+classfile_in

classifications = pd.read_csv(classfile_in)

# first, extract the started_at and finished_at from the annotations column
classifications['meta_json'] = [json.loads(q) for q in classifications.metadata]


classifications['started_at_str']  = [q['started_at']  for q in classifications.meta_json]
classifications['finished_at_str'] = [q['finished_at'] for q in classifications.meta_json]

classifications['created_day'] = [q[:10] for q in classifications.created_at]

first_class_day = min(classifications.created_day).replace(' ', '')
last_class_day  = max(classifications.created_day).replace(' ', '')


# The next thing we need to do is parse the dates into actual datetimes

# I don't remember why this is needed but I think it's faster to use this below than a for loop on the actual column
ca_temp = classifications['created_at'].copy()
sa_temp = classifications['started_at_str'].copy().str.replace('T',' ').str.replace('Z', '')
fa_temp = classifications['finished_at_str'].copy().str.replace('T',' ').str.replace('Z', '')

print "Creating timeseries..."#,datetime.datetime.now().strftime('%H:%M:%S.%f')

# Do these separately so you can track the error to a specific line
try:
    classifications['created_at_ts'] = pd.to_datetime(ca_temp, format='%Y-%m-%d %H:%M:%S %Z')
except Exception as the_error:
    print "Oops:\n", the_error
    try:
        classifications['created_at_ts'] = pd.to_datetime(ca_temp, format='%Y-%m-%d %H:%M:%S')
    except Exception as the_error:
        print "Oops:\n", the_error
        classifications['created_at_ts'] = pd.to_datetime(ca_temp)


try:
    classifications['started_at'] = pd.to_datetime(sa_temp, format='%Y-%m-%d %H:%M:%S.%f')
except Exception as the_error:
    print "Oops:\n", the_error
    try:
        classifications['started_at'] = pd.to_datetime(sa_temp, format='%Y-%m-%d %H:%M:%S %Z')
    except Exception as the_error:
        print "Oops:\n", the_error
        classifications['started_at'] = pd.to_datetime(sa_temp)


try:
    classifications['finished_at'] = pd.to_datetime(fa_temp, format='%Y-%m-%d %H:%M:%S.%f')
except Exception as the_error:
    print "Oops:\n", the_error
    try:
        classifications['finished_at'] = pd.to_datetime(fa_temp, format='%Y-%m-%d %H:%M:%S %Z')
    except Exception as the_error:
        print "Oops:\n", the_error
        classifications['finished_at'] = pd.to_datetime(fa_temp)




# save processing time and memory; only keep the columns we're going to use
classifications = classifications[cols_used]

# index by created_at as a timeseries
# note: this means things might not be uniquely indexed
# but it makes a lot of things easier and faster.
#classifications.set_index('created_at_ts', inplace=True)


all_users = classifications.user_name.unique()
by_user = classifications.groupby('user_name')


# compute the stats
print "Computing session stats for each user..."
session_stats = by_user.apply(sessionstats)

# If no stats file was supplied, add the start and end dates in the classification file to the output filename
if modstatsfile:
    statsfile_out = statsfile_out.replace('.csv', '_'+first_class_day+'_to_'+last_class_day+'.csv')

print "Writing to file", statsfile_out,"..."
session_stats.to_csv(statsfile_out)
            
