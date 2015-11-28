# older code, could use optimisation/rearrangement

import sys, os
import numpy as np
import pandas as pd
import datetime
import dateutil.parser
import csv
import json
import re, string

from ast import literal_eval

    
    
def print_by_proj(user_class_ts, all_proj, theanonid, thezooid, theuser):

    # basically do a bunch of the main calculations again, just on a per-project basis

    # print by-project session counts
    # I'm not quite sure how to do this on a group apply because I'd also have to pass the anon id?
    for thisproj in all_proj:
        sub_class = user_class_ts[user_class_ts.project_name == thisproj].copy()


        outssv_proj = '/Volumes/Brooke_SD/VOLCROWE/user_sessions/user_stats_out' + '_' + theanonid + '_' + thisproj + '.ssv'
        
        # only do this work if it hasn't already been done
        # i.e. this will not overwrite files
        # also it assumes if the ssv exists so do the by-project session files
        # so if you want to write those you'll have to erase or move the by-project ssvs
        if os.path.exists(outssv_proj):
        
            print 'Found file',outssv_proj,'for user',theuser,theanonid,thezooid,'so skipping project',thisproj
        
        else:
            
            with open(outssv_proj, 'w') as outfile:
                
                outfile.write("anon_id;n_classifications;n_sessions;unique_days;first_classification;last_classification;t_firstlast_hours;t_spent_classifying_hours;min_nclass_per_session;max_nclass_per_session;median_nclass_per_session;mean_nclass_per_session;mean_class_duration_hours;median_class_duration_hours;mean_session_length_hours;median_session_length_hours;min_session_length_hours;max_session_length_hours;mean_session_length_first2_hours;mean_session_length_last2_hours;mean_class_duration_first2_hours;mean_class_duration_last2_hours;nclass_by_session\n")
    
            # Now, define "sessions".
            sub_class['duration'] = sub_class.created_at.diff()
            sub_class['session'] = [1 for q in sub_class.project_name]
            sub_class['count'] = [1 for q in sub_class.project_name]
            sub_class['created_day'] = [q[:10] for q in sub_class.created_at_str]
            
            n_class    = len(sub_class)    
            n_days     = len(sub_class.created_day.unique())
            first_day  = sub_class.created_day[0]
            last_day   = sub_class.created_day[-1]
      
        
            # if there's only 1 classification, mostly there's no point in these stats, just set them to 0 or 1 as needed
            if n_class == 1:
                
                tdiff_firstlast_hours = 0.00
                proj_counts = 1
                
                home_proj_json = json.dumps([sub_class.project_name[0]])
                all_proj_json  = home_proj_json
                all_counts_json= json.dumps([1])
                home_proj_count = 1
    
    
                dur_median = 0.00
                dur_total = 0.00
                ses_count = 1
                ses_count_json = json.dumps(ses_count)
                n_sessions = 1
                
                count_mean = 1
                count_med  = 1
                count_min  = 1
                count_max  = 1
                
                dur_ses_mean    = 0.00
                dur_ses_median  = 0.00
                dur_ses_min     = 0.00
                dur_ses_max     = 0.00
                dur_class_total = 0.00
                
                dur_class_mean  = 0.00
                dur_class_total = 0.00
                dur_class_mean_overall = 0.00
                dur_class_median_overall = 0.00
                
                sessions_df = pd.DataFrame(data=[[1, 1]])
                sessions_df.columns = ['session', 'count']
                sessions_df.set_index('session', inplace=True)
                session_starts = pd.Series(sub_class.created_at_str[0])
                session_proj = pd.Series(sub_class.project_name[0])
                sessions_df['session_start'] = [q for q in session_starts]
                sessions_df['session_duration_hours'] = [q * ns2hours for q in pd.Series(dur_total)]
                sessions_df['project'] = [q for q in session_proj]
                                
            
            # if there are 2 or more classifications, give the stats a try        
            else:
                    
                tdiff_firstlast_hours = np.sum(sub_class.duration) * ns2hours #duration is in ns                
                
                # Figure out where new sessions start
                thefirst = sub_class.duration >= np.timedelta64(1, 'h')
                insession = np.invert(thefirst)
                n_sessions = np.sum(thefirst) + 1   #+1 b/c start of 1st session is NaN so won't be included
                starttimes = sub_class.created_at[thefirst]
                
                try:                                                                                                    
                    dur_class_mean_overall   = np.mean(sub_class.duration[insession]) /datetime.timedelta(hours=1)
                except ValueError:
                    dur_class_mean_overall   = np.nanmean(sub_class.duration[insession]/datetime.timedelta(hours=1))
                
                try:
                    if np.isnan(dur_class_mean_overall):
                        dur_class_mean_overall = 0.0
                except:
                    pass
                
                dur_class_median_overall = np.median(sub_class.duration[insession]).astype(float) * ns2hours
                try:
                    if dur_class_median_overall < 0.0:
                        dur_class_median_overall = 0.0
                except:
                    pass
                    
                    
                # now, keep the session count by adding 1 to each element of the timeseries with t > each start time
                for the_start in starttimes:
                    sub_class.session[the_start:] += 1
                
                # Now that we've defined the sessions let's do some calculations
                bysession = sub_class.groupby('session')
            
                # this will give a warning for 1-entry sessions but whatevs, let NaNs be NaNs
                # also, ignore the first duration, which isn't a real classification duration but a time between sessions
                dur_median = bysession.duration.apply(lambda x: np.median(x[1:])) /datetime.timedelta(hours=1)
                dur_total = bysession.duration.apply(lambda x: np.sum(x[1:]))  # in nanoseconds
    
                #ses_count = bysession.duration.aggregate('count') # occasional count = 0 problem as well
                ses_count = bysession.count.aggregate('sum')            
                ses_count_json = json.dumps(ses_count.tolist())
                
    
                ses_nproj = bysession.project_name.aggregate(lambda x:x.nunique())
                
                count_mean = np.mean(ses_count.astype(float))
                count_med  = np.median(ses_count)
                count_min  = np.min(ses_count)
                count_max  = np.max(ses_count)
                
                dur_ses_mean    = np.mean(dur_total) * ns2hours
                dur_ses_median  = np.median(dur_total) * ns2hours
                dur_ses_min     = np.min(dur_total) * ns2hours
                dur_ses_max     = np.max(dur_total) * ns2hours
                dur_class_total = np.sum(dur_total) * ns2hours
                
                dur_class_mean  = dur_total / ses_count.astype(float) * ns2hours
                # fix the fact that we don't have a measured classification duration for the 1st classification 
                # so we should divide by count - 1 EXCEPT in cases where count = 1
                multiclass = ses_count > 1
                dur_class_mean[multiclass] = dur_total[multiclass] / (ses_count[multiclass].astype(float) - 1.0) * ns2hours
                
    
                sessions_df = pd.DataFrame(data=pd.Series(ses_count))
                session_starts = bysession.created_at_str.head(1)
                session_proj = bysession.project_name.apply(lambda x:x.unique())
                sessions_df['session_start'] = [q for q in pd.Series(session_starts)]
                sessions_df['session_duration_hours'] = [q * ns2hours for q in pd.Series(dur_total)]
                sessions_df['project'] = session_proj
    
    
    
            
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
            
            
            ################### End session calculations
            
            
            
            # write the stats to the stats file
            with open(outssv_proj, 'a') as outfile:
                # now write to outfile
                outfile.write("%s;%.0f;%.0f;%.0f;%s;%s;%.6f;%.6f;%.0f;%.0f;%.6f;%.6f;%.6f;%.6f;%.6f;%.6f;%.6f;%.6f;%.6f;%.6f;%.6f;%.6f;%s\n" % (theanonid,n_class,n_sessions,n_days,first_day,last_day,tdiff_firstlast_hours,dur_class_total,count_min,count_max,count_med,count_mean,dur_class_mean_overall,dur_class_median_overall,dur_ses_mean,dur_ses_median,dur_ses_min,dur_ses_max,mean_duration_first2,mean_duration_last2,mean_class_duration_first2,mean_class_duration_last2,ses_count_json))
            print "   Finished",thisproj,"session calculations at ",datetime.datetime.now().strftime('%H:%M:%S.%f')
            print "   . . . . . . . . . . . . . ."
            
            
            
            # print the overall session counts
            sescount_all_file = '/Volumes/Brooke_SD/VOLCROWE/user_timeseries/anon/sessions/sessions_' + theanonid + '_'+ thisproj +'.csv'
            if not os.path.exists(sescount_all_file):
                sessions_df.to_csv(sescount_all_file)
        
    return 
    




