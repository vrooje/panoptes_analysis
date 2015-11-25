import sys
import numpy as np
import pandas as pd
import datetime
import json

# file with raw classifications (csv)
# put this way up here so if there are no inputs we exit quickly before even trying to load everything else
default_statstart = "data_out/session_stats"
default_sessionbreak = 60.

try:
    classfile_in = sys.argv[1]
except:
    print "\nUsage: {0:} classifications_infile [stats_outfile session_break_length]".format(sys.argv[0])
    print "\tclassifications_infile is a Zooniverse (Panoptes) classifications data export CSV."
    print "\tstats_outfile is the name of an outfile you'd like to write."
    print "\t\tif you don't specify one it will be {0:}_[date]_to_[date].csv".format(default_statstart)
    print "\t\twhere the dates show the first and last classification dates."
    print "\tA new session is defined to start when two classifications by the same classifier are"
    print "\t\tseparated by at least session_break_length minutes (default value: {0:.0f})".format(default_sessionbreak)
    print "\nOnly the classifications_infile is a required input.\n"
    sys.exit(0)

# timestamps & timediffs are in nanoseconds below but we want outputs in hours or minutes, depending
# Note: I'd like to keep units in days but then a session length etc in seconds is ~1e-5 and that's too
#       close to floating-point errors for my liking (because this might be read into Excel)
# we will use either this below, or
# /datetime.timedelta(hours=1)
# depending on whether the output is in a timedelta (use above) or in float (use below).
ns2hours = 1.0 / (1.0e9*60.*60.)
ns2mins  = 1.0 / (1.0e9*60.)

"""
Columns currently in an exported Panoptes classification file: 
user_name,user_id,user_ip,workflow_id,workflow_name,workflow_version,created_at,gold_standard,expert,metadata,annotations,subject_data

    - user_name is either their registered name or "not-logged-in"+their hashed IP
    - user_id is their numeric Zooniverse ID or blank if they're unregistered
    - user_ip is a hashed version of their IP
    - workflow_id is the numeric ID of this workflow, which you can find in the project builder URL
          for managing the workflow: https://www.zooniverse.org/lab/[project_id]/workflow/[workflow_id]/
    - workflow_name is the name you gave your workflow (for sanity checks)
    - workflow_version is [bigchangecount].[smallchangecount] and is probably pretty big
    - created_at is the date the entry for the classification was recorded
    - gold_standard is 1 if this classification was done in gold standard mode
    - expert is 1 if this classification was done in expert mode... I think
    - metadata (json) is the data the browser sent along with the classification. 
          Includes browser information, language, started_at and finished_at
          note started_at and finished_at are perhaps the easiest way to calculate the length of a classification
          (the duration elapsed between consecutive created_at by the same user is another way)
          the difference here is back-end vs front-end
    - annotations (json) contains the actual classification information
          which for this analysis we will ignore completely, for now
    - subject_data is cross-matched from the subjects table and is for convenience in data reduction
          here we will ignore this too, except to count subjects once.
          we'll also ignore user_ip, workflow information, gold_standard, and expert.
          some of these will be defined further down, but before we actually use this list.
"""
cols_used = ["created_at_ts", "user_name", "user_id", "created_at", "started_at", "finished_at"]

# Check for the other inputs on the command line

# Output file
try:
    # If filename is given on the command line, don't add the dates later
    statsfile = sys.argv[2]
    modstatsfile = False
except IndexError:
    statsfile = "{0:}.csv".format(default_statstart)
    modstatsfile = True

# The separation between two classifications, in minutes, that defines the start of a new session for a classifier
try:
    session_break = float(sys.argv[3])
except IndexError:
    session_break = default_sessionbreak
    
# Print out the input parameters just as a sanity check    
print "\nComputing session stats using:"
print "\tinfile: {0:}".format(classfile_in)
# If we're adding the dates to the output file, we can't print it out here because we don't yet know the dates
if not modstatsfile:
    print "\toutfile: {0:}".format(statsfile)
print "\tNew session starts after classifier break of {0:.0f} minutes\n".format(session_break)

#################################################################################
#################################################################################
#################################################################################


# This is the function that will compute the stats for each user
#
def compute_sessionstats(grp):

    # groups and dataframes behave a bit differently; life is a bit easier if we DF the group
    # also sort each individual group rather than sort the whole classification dataframe; should be much faster
    user_class = pd.DataFrame(grp).sort_values(by='created_at_ts', ascending=True)


    # If the user id is a number, great; if it's blank, keep it blank and don't force it to NaN
    try:
        theuserid = int(user_class.user_id.iloc[0])
    except:
        theuserid = user_class.user_id.iloc[0]
    
    # the next two lines are why we converted into datetime
    user_class['duration'] = user_class.created_at_ts.diff()
    user_class['class_length'] = user_class.finished_at - user_class.started_at
    # set up the session count
    user_class['session'] = [0 for q in user_class.duration]
    # because aggregate('count') has a weird bug (sometimes returns n-1 instead), just make a "count" column
    # and then aggregate('sum')
    user_class['count'] = [1 for q in user_class.duration] 
    # YYYY-MM-DD only
    user_class['created_day'] = [q[:10] for q in user_class.created_at]

    n_class    = len(user_class)    
    n_days     = len(user_class.created_day.unique())
    first_day  = user_class.created_day.iloc[0]
    last_day   = user_class.created_day.iloc[-1]

    #front-end version; back-end version uses 'created_at'
    tdiff_firstlast_hours = (user_class.finished_at[user_class.index[-1]] - user_class.started_at[user_class.index[0]]).total_seconds() / 3600.
    
    
    i_firstclass = user_class.index[0]  
    i_lastclass  = user_class.index[-1]  

    # Figure out where new sessions start, manually dealing with the first classification of the session
    thefirst = (user_class.duration >= np.timedelta64(int(session_break), 'm')) | (user_class.index == i_firstclass)
    
    # insession is more useful if for some reason you don't trust started_at and finished_at
    # and instead you need to do calculations using 'duration'
    insession = np.invert(thefirst)
    # start times for each session
    starttimes = user_class.created_at_ts[thefirst]
    # start dates for each session
    startdays  = user_class.created_day[thefirst]
    # session count; could also do sum(thefirst) but len takes less time than sum
    n_sessions = len(starttimes.unique())
    

    # timedeltas are just ints, but interpreted a certain way. So force them to int as needed.
    # By default they're in nanoseconds
    #class_length_mean_overall   = np.mean(user_class.class_length).astype(int) * ns2mins
    #class_length_median_overall = np.median(user_class.class_length).astype(int) * ns2mins
    class_length_mean_overall   = np.mean([x.seconds for x in user_class.class_length]) * 1./60
    class_length_median_overall = np.median([x.seconds for x in user_class.class_length]) * 1./60
    
    
    # index this into a timeseries
    # this means the index might no longer be unique, but it has many advantages
    user_class.set_index('created_at_ts', inplace=True, drop=False)
    
    
    # now, keep the session count by adding 1 to each element of the timeseries with t > each start time
    # not sure how to do this without a loop
    for the_start in starttimes.unique():
        user_class.session[the_start:] += 1
    
    # Now that we've defined the sessions let's do some calculations
    bysession = user_class.groupby('session')
    
    # get classification counts, total session durations, median classification length for each session
    # time units in minutes here
    # this may give a warning for 1-entry sessions but whatevs

    class_length_median = bysession.class_length.apply(lambda x: np.median(x))/datetime.timedelta(minutes=1)
    class_length_total = bysession.class_length.apply(lambda x: np.sum(x))/datetime.timedelta(minutes=1)
    class_count_session = bysession.count()['count']

    #class_length_total  = bysession.class_length.aggregate('sum') * ns2mins
    
    # make commas into semicolons because we don't want to break the eventual CSV output
    class_count_session_list = str(class_count_session.tolist()).replace(',',';')

    '''
    # below is the back-end version; use if you don't have or don't trust started_at and finished_at
    # ignore the first duration, which isn't a real classification duration but a time between sessions
    dur_median = bysession.duration.apply(lambda x: np.median(x[1:])) /datetime.timedelta(hours=1)
    dur_total = bysession.duration.apply(lambda x: np.sum(x[1:]))  # in nanoseconds
    ses_count = bysession.duration.aggregate('count')
    ses_nproj = bysession.project_name.aggregate(lambda x:x.nunique())
    '''
    
    # basic classification count stats per session
    count_mean = np.nanmean(class_count_session.astype(float))
    count_med  = np.median(class_count_session)
    count_min  = np.min(class_count_session)
    count_max  = np.max(class_count_session)
    
    session_length_mean    = np.nanmean(class_length_total).astype(float)
    session_length_median  = np.median(class_length_total).astype(float)
    session_length_min     = np.min(class_length_total)
    session_length_max     = np.max(class_length_total)
    session_length_total = np.sum(class_length_total)
     
    class_length_mean  = class_length_total * 1./class_count_session
    
    '''
    nproj_session_med  = np.median(ses_nproj)
    nproj_session_mean = np.nanmean(ses_nproj.astype(float))
    nproj_session_min  = np.min(ses_nproj)
    nproj_session_max  = np.max(ses_nproj)
    '''
    if n_sessions >= 4:
        # get durations of first two and last two sessions
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
    session_stats_dict = {}
    session_stats_dict["user_id"]                              = theuserid # note: username will be in the index, this is zooid
    session_stats_dict["n_class"]                              = n_class
    session_stats_dict["n_sessions"]                           = n_sessions
    session_stats_dict["n_days"]                               = n_days
    session_stats_dict["first_day"]                            = first_day
    session_stats_dict["last_day"]                             = last_day
    session_stats_dict["tdiff_firstlast_hours"]                = tdiff_firstlast_hours             # hours
    session_stats_dict["time_spent_classifying_total_minutes"] = session_length_total              # minutes
    session_stats_dict["class_per_session_min"]                = count_min
    session_stats_dict["class_per_session_max"]                = count_max
    session_stats_dict["class_per_session_med"]                = count_med
    session_stats_dict["class_per_session_mean"]               = count_mean
    session_stats_dict["class_length_mean_overall"]            = float(class_length_mean_overall)  # minutes
    session_stats_dict["class_length_median_overall"]          = class_length_median_overall       # minutes
    session_stats_dict["session_length_mean"]                  = session_length_mean               # minutes
    session_stats_dict["session_length_median"]                = session_length_median             # minutes
    session_stats_dict["session_length_min"]                   = session_length_min                # minutes
    session_stats_dict["session_length_max"]                   = session_length_max                # minutes
    session_stats_dict["mean_session_length_first2"]           = mean_duration_first2              # minutes
    session_stats_dict["mean_session_length_last2"]            = mean_duration_last2               # minutes
    session_stats_dict["mean_class_length_first2"]             = mean_class_duration_first2        # minutes
    session_stats_dict["mean_class_length_last2"]              = mean_class_duration_last2         # minutes   
    session_stats_dict["class_count_session_list"]             = class_count_session_list          # semicolon-separated


    s = session_stats_dict

    return s


#################################################################################
#################################################################################
#################################################################################

def gini(list_of_values):
    """
    Get the Gini coefficient - https://en.wikipedia.org/wiki/Gini_coefficient
    
    The Gini coefficient measures inequality in distributions of things.
    It was originally conceived for economics (e.g. where is the wealth in a country?
     in the hands of many citizens or a few?), but it's just as applicable to many
     other fields. In this case we'll use it to see how classifications are
     distributed among classifiers.
    G = 0 is a completely even distribution (everyone does the same number of 
     classifications), and ~1 is uneven (~all the classifications are done
     by one classifier). 
    Typical values of the Gini for healthy Zooniverse projects (Cox et al. 2015) are
     in the range of 0.7-0.9.
    That range is generally indicative of a project with a loyal core group of 
       volunteers who contribute the bulk of the classification effort, but balanced
       out by a regular influx of new classifiers trying out the project, from which
       you continue to draw to maintain a core group of prolific classifiers.
    Once your project is fairly well established, you can compare it to past Zooniverse
     projects to see how you're doing. 
     If your G is << 0.7, you may be having trouble recruiting classifiers into a loyal 
       group of volunteers. 
     If your G is > 0.9, it's a little more complicated. If your total classification
       count is lower than you'd like it to be, you may be having trouble recruiting
       classifiers to the project, such that your classification counts are
       dominated by a few people.
     But if you have G > 0.9 and plenty of classifications, this may be a sign that your
       loyal users are -really- committed, so a very high G is not necessarily a bad thing.
    
    Of course the Gini coefficient is a simplified measure that doesn't always capture
     subtle nuances and so forth, but it's still a useful broad metric.
    """
    sorted_list = sorted(list_of_values)
    height, area = 0, 0
    for value in sorted_list:
        height += value
        area += height - value / 2.
    fair_area = height * len(list_of_values) / 2
    return (fair_area - area) / fair_area
    
#################################################################################
#################################################################################
#################################################################################

def df_fix(series):

    # Turn the Series of dictionaries into an ordered DataFrame

    d = {}
    keys = series[0].keys()

    for k in keys:
        d[k] = []

    for s in series:
        for k in keys:
            d[k].append(s[k])

    # Manually order the column list
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

    df = pd.DataFrame(d,index=series.index)

    # Check that all columns desired are present
    co1 = np.array(col_order).sort()
    co2 = np.array(df.columns).sort()
    if np.all(co1 == co2):
        df = df[col_order]
    else:
        print("Column order does not match list given.")
    
    return df


#################################################################################
#################################################################################
#################################################################################

# Begin the main routine

def run_main():
    
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
    
    # Do these separately so you can track errors to a specific line
    # Try the format-specified ones first (because it's faster, if it works)
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
    # though before we do that, grab the subject count
    n_subj_tot  = len(classifications.subject_data.unique())
    classifications = classifications[cols_used]
    
    all_users = classifications.user_name.unique()
    by_user = classifications.groupby('user_name')
    
    # get some basic overall stats
    n_class_tot = len(classifications)
    n_users_tot = len(all_users)
    
    unregistered = [q.startswith("not-logged-in") for q in all_users]
    n_unreg = sum(unregistered)
    n_reg   = n_users_tot - n_unreg
    
    # for the leaderboard, which I recommend project builders never make public because 
    # Just Say No to gamification
    # But it's still interesting to see who your most prolific classifiers are, and
    # e.g. whether they're also your most prolific Talk users
    nclass_byuser = by_user.created_at.aggregate('count')
    nclass_byuser_ranked = nclass_byuser.copy()
    nclass_byuser_ranked.sort_values(inplace=True,ascending=False)
    
    # very basic stats
    nclass_med    = np.median(nclass_byuser)
    nclass_mean   = np.mean(nclass_byuser)
    
    # Gini coefficient - see the comments above the gini() function for more notes
    nclass_gini   = gini(nclass_byuser)
    ntop = 10
    
    print "\nOverall:\n\n{0:.0f} classifications of {1:.0f} subjects by {2:.0f} classifiers,".format(n_class_tot,n_subj_tot,n_users_tot)
    print "with {0:.0f} registered users and {1:.0f} unregistered users.\n".format(n_reg,n_unreg)
    print "Median number of classifications per user: {0:.0f}".format(nclass_med)
    print "Mean number of classifications per user: {0:.1f}".format(nclass_mean)
    print "\n{0:.0f} most prolific classifiers:\n".format(ntop),nclass_byuser_ranked.head(ntop)
    print "\n\nGini coefficient for classifications by user: {0:.2f}\n".format(nclass_gini)
    
    # compute the per-user stats
    # alas I don't know of a way to print a progress bar or similar for group.apply() functions
    # For a small classification file this is fast, but if you have > 1,000,000 this may be slow
    #  (albeit still much faster than a loop or similar)

    print "\nComputing session stats for each user..."
    session_stats_series = by_user.apply(compute_sessionstats)

    session_stats_df = df_fix(session_stats_series)

    # If no stats file was supplied, add the start and end dates in the classification file to the output filename
    statsfile_out = statsfile.replace('.csv','_{0:s}_to_{1:s}.csv'.format(first_class_day,last_class_day)) if modstatsfile else statsfile
    
    print "Writing to file {0:} ...".format(statsfile_out)

    print "Session stats type:",type(session_stats_df)
    session_stats_df.to_csv(statsfile_out)

    return None

def test_versions():

    filename = 'session_stats_2015-05-26_to_2015-11-19.csv' 
    kyle_file = 'data_out/%s' % filename
    brooke_file = 'brooke/%s' % filename

    kd = pd.read_csv(kyle_file)
    bd = pd.read_csv(brooke_file)

    cols = kd.columns

    name = 'zookeeper'
    bad = 0
    for c in cols:
        bcol = bd[bd['user_name'] == name][c].values[0]
        kcol = kd[kd['user_name'] == name][c].values[0]

        # Round off values to some precision e
        e = 1e-4
        if type(kcol) == np.float64:
            kcol = round(kcol*1./e) * e
            bcol = round(bcol*1./e) * e

        if kcol != bcol:
            print c
            print "\tBrooke's code: {0:}".format(bcol)
            print "\tKyle's   code: {0:}".format(kcol)
            bad += 1

    print "Missed {0:}/{1:} columns".format(bad,len(cols))
    return None

if __name__ == "__main__":
    run_main()
    test_versions()
