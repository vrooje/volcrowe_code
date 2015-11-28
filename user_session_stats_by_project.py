import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import datetime
import dateutil.parser
import csv
import json
#import mysql.connector
#import pymysql

from ast import literal_eval  # but for json don't use this, use json.loads
from anon_id_utils import get_anon_id



# timestamps & timediffs are in nanoseconds below but we want outputs in HOURS
# Note: I'd like to keep units in days but then a session length etc in seconds is ~1e-5 and that's too
#       close to floating-point errors for my liking (because this might be read into Excel)
# we will use either this below, or
# /datetime.timedelta(hours=1)
# depending on whether the output is in a timedelta (use above) or in float (use below).
ns2hours = 1.0 / (1.0e9*60.*60.)




userid = 'zooniverse_user_id'

try:
    sys.argv[1]
except IndexError:
    project_name = 'GZ4'
else:
    project_name = sys.argv[1]

# file with raw classifications (csv)
try:
    sys.argv[2]
except IndexError:
    #galaxyzoo_all_classifications_from_logged_in_users_withanonid_2014-10-02.csv
    #penguins_classifications_withanonid_1classificationpersubjectanduser.csv
    #planethunters_all_classifications_from_logged_in_users_withanonid_2014-09-14.csv
    #seafloor_all_classifications_from_logged_in_users_withanonid_2014-09-24.csv
    #serengeti_all_classifications_from_logged_in_users_withanonid_2014-09-21.csv
    # all have the needed columns and not much else
    # (zoo user id, anon_id, created_at)
    #
    classifications_filename = 'galaxyzoo_all_classifications_from_logged_in_users_withanonid_2014-10-02.csv'
else:
    classifications_filename = sys.argv[2]

try:
    sys.argv[3]
except IndexError:
    #filestr_base = "/Volumes/Brooke_SD/VOLCROWE/user_stats_session_byproject_out"
    filestr_base = "user_stats_session_"+project_name+"_out.csv"
else:
    filestr_base = sys.argv[3]






def sessionstats(grp):

    theanonid = grp.anon_id[grp.index[0]]
    thezooid  = get_username(theanonid)

    # now make this into a timeseries so we can calculate sessions
    #print "Creating time series for "+theanonid+"...",datetime.datetime.now().strftime('%H:%M:%S.%f')
    user_class_timeseries = grp.sort('created_at_ts', ascending=True)
    user_class_timeseries.set_index('created_at_ts', drop=False, inplace=True)    
    #print "Time series created. Beginning session calculations...", datetime.datetime.now().strftime('%H:%M:%S.%f')
    
    # Might be good to check to see if the timeseries is actually sorted and formatted correctly
    # -- this is very finicky and if the date formatting has e.g. an extra ' ' or the sql outputs it
    # as bytechar (result of TIMESTAMP formatting in db) instead of string, it can go haywire.
    
#with open(filestr_base+".ssv", 'w') as outfile:
    
    # Now, define "sessions".
    user_class_timeseries['duration'] = user_class_timeseries.created_at_ts.diff()
    user_class_timeseries['session'] = [1 for q in user_class_timeseries.duration]
    user_class_timeseries['created_day'] = [q[:10] for q in user_class_timeseries.created_at]
    
    n_class    = len(user_class_timeseries)    
    n_days     = len(user_class_timeseries.created_day.unique())
    first_day  = user_class_timeseries.created_day[0]
    last_day   = user_class_timeseries.created_day[-1]
    if n_class > 1:
        tdiff_firstlast_hours = np.sum(user_class_timeseries.duration).astype(float) * ns2hours #duration is in ns
    else:
        tdiff_firstlast_hours = 0.0
        
#     # we want the first project the user classified on, the last one they classified on,
#     #  ALL the projects they classified on, and their "home" project(s) (proj w/ max classifications).
#     first_proj = user_class_timeseries.project_name[0]
#     last_proj  = user_class_timeseries.project_name[-1]
#     all_proj   = user_class_timeseries.project_name.unique()
#     n_proj     = len(all_proj)
#     #all_proj_json = json.dumps(all_proj.tolist())
#     byproject = user_class_timeseries.groupby('project_name')
#     proj_counts = byproject.duration.aggregate('count')
#     home_proj_count = max(proj_counts)
#     home_proj_json = json.dumps(proj_counts[proj_counts == max(proj_counts)].index.tolist())
#     all_proj_json  = json.dumps(proj_counts.index.tolist())
#     all_counts_json= json.dumps(proj_counts.tolist())
#     # could also output json.dumps(proj_counts.index.tolist()), json.dumps(proj_counts.tolist())
    
    i_firstclass = user_class_timeseries.index[0]  # these times aren't necessarily unique (if a user's browser glitches)
    id_firstclass = user_class_timeseries.ix[0].id # the classification id "id" is more unique
                                                   # we're not indexing by it because a timeseries is way better for us
    # Figure out where new sessions start, manually dealing with the first classification of the session
    thefirst = (user_class_timeseries.duration >= np.timedelta64(1, 'h')) | (user_class_timeseries.id == id_firstclass)
    
    insession = np.invert(thefirst)
    starttimes = user_class_timeseries.created_at_ts[thefirst]
    n_sessions = len(starttimes.unique())
    
    if n_class > 1:
        dur_class_mean_overall   = np.nanmean(user_class_timeseries.duration[insession]).astype(float) * ns2hours
        dur_class_median_overall = np.median(user_class_timeseries.duration[insession]).astype(float) * ns2hours
    else:
        dur_class_mean_overall   = 0.0
        dur_class_median_overall = 0.0
        
    user_class_timeseries.session = user_class_timeseries.session * 0
    # now, keep the session count by adding 1 to each element of the timeseries with t > each start time
    for the_start in starttimes.unique():
        user_class_timeseries.session[the_start:] += 1
    
    
    # Now that we've defined the sessions let's do some calculations
    bysession = user_class_timeseries.groupby('session')
    
    # this will give a warning for 1-entry sessions but whatevs, let NaNs be NaNs
    # also, ignore the first duration, which isn't a real classification duration but a time between sessions
    dur_median = bysession.duration.apply(lambda x: np.median(x[1:])) /datetime.timedelta(hours=1)
    dur_total = bysession.duration.apply(lambda x: np.sum(x[1:]))  # in nanoseconds
    ses_count = bysession.duration.aggregate('count')
#    ses_nproj = bysession.project_name.aggregate(lambda x:x.nunique())
    
    count_mean = np.nanmean(ses_count.astype(float))
    count_med  = np.median(ses_count)
    count_min  = np.min(ses_count)
    count_max  = np.max(ses_count)
    
    if n_class > 1:
        dur_ses_mean    = np.nanmean(dur_total).astype(float) * ns2hours
        dur_ses_median  = np.median(dur_total).astype(float) * ns2hours
        dur_ses_min     = np.min(dur_total) * ns2hours
        dur_ses_max     = np.max(dur_total) * ns2hours
        dur_class_total = np.sum(dur_total) * ns2hours
    else:
        dur_ses_mean    = 0.0
        dur_ses_median  = 0.0
        dur_ses_min     = 0.0
        dur_ses_max     = 0.0
        dur_class_total = 0.0
        
    dur_class_mean  = dur_total / ses_count.astype(float) * ns2hours
    # fix the fact that we don't have a measured classification duration for the 1st classification 
    # so we should divide by count - 1 EXCEPT in cases where count = 1
    multiclass = ses_count > 1
    dur_class_mean[multiclass] = dur_total[multiclass] / (ses_count[multiclass].astype(float) - 1.0) * ns2hours
    
#     nproj_session_med  = np.median(ses_nproj)
#     nproj_session_mean = np.nanmean(ses_nproj.astype(float))
#     nproj_session_min  = np.min(ses_nproj)
#     nproj_session_max  = np.max(ses_nproj)
    
    
    if n_sessions >= 4:
        # get durations of first 2 and last 2 sessions
        # note I have reservations about including this for stats that may include multiple projects
        #   be VERY careful interpreting changes in these, in that case.
        mean_duration_first2 = (dur_total[1]+dur_total[2])/2.0 * ns2hours
        mean_duration_last2  = (dur_total[n_sessions]+dur_total[n_sessions-1])/2.0 * ns2hours
        mean_class_duration_first2 = (dur_total[1]+dur_total[2])/(ses_count[1]+ses_count[2]).astype(float) * ns2hours
        mean_class_duration_last2  = (dur_total[n_sessions]+dur_total[n_sessions-1])/(ses_count[n_sessions]+ses_count[n_sessions-1]).astype(float) * ns2hours
    else:
        mean_duration_first2 = 0.0
        mean_duration_last2  = 0.0
        mean_class_duration_first2 = 0.0
        mean_class_duration_last2  = 0.0
    
    with open(filestr_base, 'a') as outfile:
        # now write to outfile
        outfile.write("%.0f,%s,%.0f,%.0f,%.0f,%s,%s,%.6f,%.6f,%.0f,%.0f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f\n" % (thezooid,theanonid,n_class,n_sessions,n_days,first_day,last_day,tdiff_firstlast_hours,dur_class_total,count_min,count_max,count_med,count_mean,dur_class_mean_overall,dur_class_median_overall,dur_ses_mean,dur_ses_median,dur_ses_min,dur_ses_max,mean_duration_first2,mean_duration_last2,mean_class_duration_first2,mean_class_duration_last2))
    #print "Finished session calculations at ",datetime.datetime.now().strftime('%H:%M:%S.%f')
    #print "----------------------------------------"

    # End user session calculations
#########################################################################
#########################################################################
#########################################################################
#########################################################################









# Begin the main stuff

print "Reading classifications for "+project_name+" from "+classifications_filename
classifications = pd.read_csv(classifications_filename)

date_temp = classifications['created_at'].copy()


print "Creating timeseries...",datetime.datetime.now().strftime('%H:%M:%S.%f')
try:
    classifications['created_at_ts'] = pd.to_datetime(date_temp, format='%Y-%m-%d %H:%M:%S %Z')
except ValueError:
    classifications['created_at_ts'] = pd.to_datetime(date_temp, format='%Y-%m-%d %H:%M:%S')


print "Starting grouping and sessions...",datetime.datetime.now().strftime('%H:%M:%S.%f')
with open(filestr_base, 'w') as outfile:
    
    outfile.write("zooniverse_user_id,anon_id,n_classifications,n_sessions,unique_days,first_classification,last_classification,t_firstlast_hours,t_spent_classifying_hours,min_nclass_per_session,max_nclass_per_session,median_nclass_per_session,mean_nclass_per_session,mean_class_duration_hours,median_class_duration_hours,mean_session_length_hours,median_session_length_hours,min_session_length_hours,max_session_length_hours,mean_session_length_first2_hours,mean_session_length_last2_hours,mean_class_duration_first2_hours,mean_class_duration_last2_hours\n")


by_user = classifications.groupby(userid)

#this should write the file etc.
by_user.apply(sessionstats)

print "...Results finished writing to "+filestr_base+" at",datetime.datetime.now().strftime('%H:%M:%S.%f')
print "-------------------------------"

            
            
            
