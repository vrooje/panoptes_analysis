# panoptes_analysis
Tools for analysis of classification and subject data from github.com/zooniverse/Panoptes

 - sessions_inproj_byuser.py - computes classification and session statistics for classifiers. Run at the the command line without additional inputs to see the usage. *Output columns:*
    - *n_class:* total number of classifications by the classifier
    - *n_sessions:* total number of sessions by the classifier
    - *n_days:* number of unique days on which the classifier has classified
    - *first_day:* date of first classification (YYYY-MM-DD)
    - *last_day:* date of last classification (YYYY-MM-DD)
    - *tdiff_firstlast_hours:* time elapsed between first and last classification (hours)
    - *time_spent_classifying_total_minutes:* total time spent actually classifying, i.e. work effort (minutes)
    - *class_per_session_min:* minimum number of classifications per session
    - *class_per_session_max:* maximum number of classifications per session
    - *class_per_session_med:* median number of classifications per session
    - *class_per_session_mean:* mean number of classifications per session
    - *class_length_mean_overall:* mean length of a single classification (minutes), over all sessions
    - *class_length_median_overall:* median length of a single classification (minutes), over all sessions
    - *session_length_mean:* mean length of a session (minutes)
    - *session_length_median:* median length of a session (minutes)
    - *session_length_min:* length of shortest session (minutes)
    - *session_length_max:* length of longest session (minutes)
    - *mean_session_length_first2:* mean session length in the classifier's first 2 sessions (minutes)
    - *mean_session_length_last2:* mean session length in the classifier's last 2 sessions (minutes)
    - *mean_class_length_first2:* mean classification length in the classifier's first 2 sessions (minutes)
    - *mean_class_length_last2:* mean classification length in the classifier's last 2 sessions (minutes)
    - *class_count_session_list:* classification counts in each session, formatted as: [n_class_1; n_class_2; ...]

The mean session and classification lengths in the first 2 and last 2 sessions are only calculated if the user has classified in at least 4 sessions; otherwise the values are 0.
