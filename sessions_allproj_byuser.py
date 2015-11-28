import sys, os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import datetime
import dateutil.parser
import csv
import json
#import mysql.connector
import pymysql
import re, string

from ast import literal_eval
from anon_id_utils import get_anon_id

try:
    from astropy.io import fits as pyfits
    from astropy.io.fits import Column
    from astropy.io import ascii
except ImportError:
    import pyfits
    from pyfits import Column

input print_stats_by_proj_session.py

# timestamps & timediffs are in nanoseconds below but we want outputs in HOURS
# Note: I'd like to keep units in days but then a session length etc in seconds is ~1e-5 and that's too
#       close to floating-point errors for my liking (because this might be read into Excel)
# we will use either this below, or
# /datetime.timedelta(hours=1)
# depending on whether the output is in a timedelta (use above) or in float (use below).
ns2hours = 1.0 / (1.0e9*60.*60.)


try:
    # dbconfig = {
    #   'user': 'root',
    #   'password': '',
    #   'host': '127.0.0.1',
    #   'raise_on_warnings': True,
    # }
    
    #the_cnx = mysql.connector.connect(**dbconfig)
    the_cnx = pymysql.connect(user='root',passwd='',host='127.0.0.1')
    cursor = the_cnx.cursor()
    dbconn = True
except:
    print "WARNING: error with MySQL connection, if you need it later your program is going to crash..."
    dbconn = False





get_all = 0


print "Getting users..."
if get_all:
    # this is for ALL users in the zooniverse
    #and is gonna take a loooooong time
    
    cursor = the_cnx.cursor()
    getusers = "select login as user_name, id as zooniverse_id from zoohome_300315.users"
    cursor.execute(getusers)
    the_users = pd.DataFrame(cursor.fetchall())
    the_users.columns = cursor.column_names
    the_users['anon_id'] = [get_anon_id(q) for q in the_users.zooniverse_id]
    cursor.close()

else:
    user_file = '/Volumes/Brooke_SD/VOLCROWE/survey/survey_answered_ids_CONFIDENTIAL.csv'
    the_users = pd.read_csv(user_file)
    the_users.set_index('zooniverse_id', drop=False, inplace=True)


filestr_base = "/Volumes/Brooke_SD/VOLCROWE/user_stats_out_new"

# if starting from scratch
starting_row = 121

# if you got interrupted
#starting_row = 1665

#only start a new file and write the header if you're starting from scratch
if starting_row == 0:
    
    with open(filestr_base+".ssv", 'w') as outfile:
        
        outfile.write("anon_id;n_classifications;n_sessions;n_projects;unique_days;first_classification;last_classification;t_firstlast_hours;t_spent_classifying_hours;min_nclass_per_session;max_nclass_per_session;median_nclass_per_session;mean_nclass_per_session;mean_class_duration_hours;median_class_duration_hours;mean_session_length_hours;median_session_length_hours;min_session_length_hours;max_session_length_hours;mean_session_length_first2_hours;mean_session_length_last2_hours;mean_class_duration_first2_hours;mean_class_duration_last2_hours;min_nproj_per_session;max_nproj_per_session;median_nproj_per_session;mean_nproj_per_session;n_class_in_home_project;home_project;all_projects;nclass_by_project;nclass_by_session\n")







#GZ2 and GZ3 - Feb 2009
#SSW - Feb 2010
#Moon Zoo - May 2010
#MWP1 - Dec 2010
#PH1 - December 2010
#Ancient Lives - July 2011
#    Note I'm counting each row from the transcriptions table as a classification,
#    although a transcription may be made of many markers etc.
#WhaleFM - Nov 2011
#    this is weird b/c there are logged-in people who don't have a zoo user id. WUT
oldproj = [["galaxy_zoo", "zoohome_300315.gz2_gz3_classifications_basic"], ["solarstormwatch", "SSW_20140914.classifications"], ["moon_zoo", "moonzoo_20140914.classifications"], ["milky_way", "milkyway_20140914.classifications"], ["planet_hunter", "planethunters_20140914.classifications"], ["ancient_lives", "AncientLives_20140914.transcriptions_basic_withzooid"], ["whales", "whales_20140914.classifications_basic"]]


# alphabetical is nice to read but not as efficient as chronological
# note this planet_hunter is not the original PH; same with milky way and OW
#ouroboros_tables = "andromeda asteroid bat_detective cancer_cells chicago chimp condor crater cyclone_center galaxy_zoo galaxy_zoo_starburst higgs_hunter illustratedlife kelp leaf m83 milky_way notes_from_nature oldweather orchid penguin planet_four planet_hunter plankton radio sea_floor serengeti spacewarp sunspot war_diary wisconsin wise worms".split()

ouroboros_tables = "galaxy_zoo galaxy_zoo_starburst milky_way oldweather sea_floor cyclone_center bat_detective cancer_cells andromeda serengeti planet_four notes_from_nature spacewarp leaf worms plankton radio war_diary m83 wise sunspot condor asteroid kelp penguin higgs_hunter chicago planet_hunter illustratedlife chimp orchid wisconsin crater".split()





# just for testing, use myself
theuser  = 'vrooje'
thezooid = 110839
theanonid = 'f1ce3f4'
    
for i, thezooid in enumerate(the_users.zooniverse_id):
    theuser   = the_users[the_users.zooniverse_id==thezooid].user_name[thezooid]
    theanonid = the_users[the_users.zooniverse_id==thezooid].anon_id[thezooid]
        
    user_class = 0 # really be sure this thing is empty
    # then
    # create empty dataframe to minimize errors
    user_class = pd.DataFrame(columns="created_at project_name".split())

    if i < starting_row:
        print "Skipping user",theuser,"("+theanonid+")"
    else:
        #if i == len(the_users) % 20:
        print "User",i,"of",len(the_users),"("+theuser+", "+theanonid+", "+str(thezooid)+")",datetime.datetime.now().strftime('%H:%M:%S.%f')
        
        
        
        #Check to see if a timeseries file already exists for them. If it does, read that.
        #If it doesn't, do this whole database query thing (takes ~7m per user)

        # remove all non-alphanumeric characters from the username (so it doesn't break filenames)
        # no underscores allowed
        #pattern = re.compile('[\W_]+', re.UNICODE)
        # underscores ok, question marks ok
        #pattern = re.compile('\W+', re.UNICODE)
        # underscores ok but question marks not ok
        pattern = re.compile('[\W\xe9]+', re.UNICODE)
        theuserclean = pattern.sub('', theuser)
    
        usertsfile = '/Volumes/Brooke_SD/VOLCROWE/user_timeseries/timeseries_'+str(thezooid)+'_'+theuserclean+'.csv'
        try:
            userts = pd.read_csv(usertsfile)
            #print "Read time series from", usertsfile
            newtsneeded = False
            
            user_class = userts['created_at_str project_name'.split()]  #timeseries needs to have at least these columns
            try:
                user_class['created_at'] = [pd.to_datetime(q, format='%Y-%m-%d %H:%M:%S.%f') for q in user_class.created_at_str]
            except ValueError:
                try:
                    user_class['created_at'] = [pd.to_datetime(q, format='%Y-%m-%d %H:%M:%S') for q in user_class.created_at_str]
                except ValueError:
                    user_class['created_at'] = [pd.to_datetime(q) for q in user_class.created_at_str]
            
            # user_class should have created_at, created_at_str, project_name as columns
            # we are assuming that the timeseries was pre-sorted by created_at before it was written originally
            # so we don't need to waste CPU cycles sorting it again
            user_class_timeseries = user_class

            
            
        except:
            print "Querying databases..."
            newtsneeded = True
        
            #GZ1 - July 2007
            #print "Starting at",datetime.datetime.now().strftime('%H:%M:%S.%f')
            print 'Galaxy Zoo 1 -',theuser
            # this table has all the usernames in quotes because FML
            cursor = the_cnx.cursor()
            
            # let us count ALLLLL the ways totally unrestricted username characters try to screw us over in post-processing
            if theuser.find("'") >= 0:
                theuser2 = theuser.replace("'", "\\'")
            else:
                theuser2 = theuser
            
            query = "select replace(created_at, '\"', '') as created_at_str, 'galaxy_zoo' as project_name from zoohome_300315.gz1_classifications_basic where user_name = '\""+theuser2+"\"'"
            #query = "select replace(created_at, '\"', '') as created_at_str, 'galaxy_zoo' as project_name from gz1_full.gz1_namesubjdateonly where user_name = '\""+theuser+"\"'"
            cursor.execute(query)
            aa = cursor.fetchall()
            #qq = pd.DataFrame(aa)
            if len(aa) > 0:
                qq = pd.DataFrame(np.array(aa),columns='created_at_str project_name'.split())
                #qq.columns = cursor.column_names
                try:
                    qq['created_at'] = [pd.to_datetime(q, format='%Y-%m-%d %H:%M:%S') for q in qq.created_at_str]
                except ValueError:
                    qq['created_at'] = [pd.to_datetime(q) for q in qq.created_at_str]
    
                user_class = user_class.append(qq)
            cursor.close()
            
            # Loop through pre-ouroboros projects
            for projpair in oldproj:
                print projpair[0], "-", theuser
                cursor = the_cnx.cursor()
                query = "select convert(replace(created_at, '\"', '') using latin1) as created_at_str, '"+projpair[0]+"' as project_name from "+projpair[1]+" where zooniverse_user_id = "+str(thezooid)
                #print query
                cursor.execute(query)
                aa = cursor.fetchall()
                #qq = pd.DataFrame(aa)
                if len(aa) > 0:
                    qq = pd.DataFrame(np.array(aa),columns='created_at_str project_name'.split())
                    #qq.columns = cursor.column_names
                    try:
                        qq['created_at'] = [pd.to_datetime(q, format='%Y-%m-%d %H:%M:%S') for q in qq.created_at_str]
                    except ValueError:
                        qq['created_at'] = [pd.to_datetime(q) for q in qq.created_at_str]
    
                    user_class = user_class.append(qq)
                cursor.close()
            
            
            # Loop through ouroboros projects
            for thetable in ouroboros_tables:
                print thetable,'-',theuser
                cursor = the_cnx.cursor()
                query = "select created_at_new as created_at_str, '"+thetable+"' as project_name from classifications_all_20150731."+thetable+" where zooniverse_id = "+str(thezooid)
                cursor.execute(query)
                aa = cursor.fetchall()
                #qq = pd.DataFrame(cursor.fetchall())
                if len(aa) > 0:
                    qq = pd.DataFrame(np.array(aa),columns='created_at_str project_name'.split())
                    #qq.columns = cursor.column_names
                    try:
                        qq['created_at'] = [pd.to_datetime(q, format='%Y-%m-%d %H:%M:%S.%f') for q in qq.created_at_str]
                    except ValueError:
                        try:
                            qq['created_at'] = [pd.to_datetime(q, format='%Y-%m-%d %H:%M:%S') for q in qq.created_at_str]
                        except ValueError:
                            qq['created_at'] = [pd.to_datetime(q) for q in qq.created_at_str]
                    user_class = user_class.append(qq)    
                cursor.close()
            
            print "Querying finished at",datetime.datetime.now().strftime('%H:%M:%S.%f')
                        
            # no Panoptes projects included    at this time
           
            # now make this into a timeseries so we can calculate sessions
            print "Creating time series..."    #,datetime.datetime.now().strftime('%H:%M:%S.%f')
            
            user_class_timeseries = user_class.sort('created_at', ascending=True)
            # END read-from-file vs query-database 
            print "Beginning session calculations...", datetime.datetime.now().strftime('%H:%M:%S.%f')
        
        
        
        user_class_timeseries.set_index('created_at', drop=False, inplace=True)
        
        # Might be good to check to see if the timeseries is actually sorted and formatted correctly
        # -- this is very finicky and if the date formatting has e.g. an extra ' ' or the sql outputs it
        # as bytechar (result of TIMESTAMP formatting in db) instead of string, it can go haywire.
        
        #with open(filestr_base+".ssv", 'w') as outfile:
        
        # Now, define "sessions".
        user_class_timeseries['duration'] = user_class_timeseries.created_at.diff()
        user_class_timeseries['session'] = [1 for q in user_class_timeseries.project_name]
        user_class_timeseries['count'] = [1 for q in user_class_timeseries.project_name]
        user_class_timeseries['created_day'] = [q[:10] for q in user_class_timeseries.created_at_str]
        
        n_class    = len(user_class_timeseries)    
        n_days     = len(user_class_timeseries.created_day.unique())
        first_day  = user_class_timeseries.created_day[0]
        last_day   = user_class_timeseries.created_day[-1]
        
        # we want the first project the user classified on, the last one they classified on,
        #  ALL the projects they classified on, and their "home" project(s) (proj w/ max classifications).
        first_proj = user_class_timeseries.project_name[0]
        last_proj  = user_class_timeseries.project_name[-1]
        all_proj   = user_class_timeseries.project_name.unique()
        n_proj     = len(all_proj)

        
        # if there's only 1 classification, mostly there's no point in these stats, just set them to 0 or 1 as needed
        if n_class == 1:
            
            tdiff_firstlast_hours = 0.00
            proj_counts = 1
            
            home_proj_json = json.dumps([user_class_timeseries.project_name[0]])
            all_proj_json  = home_proj_json
            all_counts_json= json.dumps([1])
            home_proj_count = 1


            dur_median = 0.00
            dur_total = 0.00
            ses_count = 1
            ses_count_json = json.dumps(ses_count)
            n_sessions = 1
            ses_nproj = 1
            
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
            
            nproj_session_med  = 1
            nproj_session_mean = 1
            nproj_session_min  = 1
            nproj_session_max  = 1

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
                
            tdiff_firstlast_hours = np.sum(user_class_timeseries.duration) * ns2hours #duration is in ns
            
            #all_proj_json = json.dumps(all_proj.tolist())
            byproject = user_class_timeseries.groupby('project_name')
            
            #sometimes this gives zero values, which I don't get - but I'll just work around it with the 'count' column
            #proj_counts = byproject.duration.aggregate('count')
            proj_counts = byproject.count.aggregate('sum')

            home_proj_count = max(proj_counts)
            home_proj_json = json.dumps(proj_counts[proj_counts == max(proj_counts)].index.tolist())
            all_proj_json  = json.dumps(proj_counts.index.tolist())
            all_counts_json= json.dumps(proj_counts.tolist())
            # could also output json.dumps(proj_counts.index.tolist()), json.dumps(proj_counts.tolist())
            
            
            # Figure out where new sessions start
            thefirst = user_class_timeseries.duration >= np.timedelta64(1, 'h')
            insession = np.invert(thefirst)
            n_sessions = np.sum(thefirst) + 1   #+1 b/c start of 1st session is NaN so won't be included
            starttimes = user_class_timeseries.created_at[thefirst]
            
            try:                                                                                                    
                dur_class_mean_overall   = np.mean(user_class_timeseries.duration[insession]) /datetime.timedelta(hours=1)
            except ValueError:
                dur_class_mean_overall   = np.nanmean(user_class_timeseries.duration[insession]/datetime.timedelta(hours=1))
            
            try:
                if np.isnan(dur_class_mean_overall):
                    dur_class_mean_overall = 0.0
            except:
                pass
            
            dur_class_median_overall = np.median(user_class_timeseries.duration[insession]).astype(float) * ns2hours
            try:
                if dur_class_median_overall < 0.0:
                    dur_class_median_overall = 0.0
            except:
                pass
                
                
            # now, keep the session count by adding 1 to each element of the timeseries with t > each start time
            for the_start in starttimes:
                user_class_timeseries.session[the_start:] += 1
            
            # Now that we've defined the sessions let's do some calculations
            bysession = user_class_timeseries.groupby('session')
        
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
            
            nproj_session_med  = np.median(ses_nproj)
            nproj_session_mean = np.mean(ses_nproj.astype(float))
            nproj_session_min  = np.min(ses_nproj)
            nproj_session_max  = np.max(ses_nproj)

            sessions_df = pd.DataFrame(data=pd.Series(ses_count))
            session_starts = bysession.created_at_str.head(1)
            try:
                # sometimes this works, other times it throws a weird exception?
                session_proj = bysession.project_name.apply(lambda x:pd.Series(x).unique())
            except:
                sproj = pd.Series(bysession.project_name) # makes a Series of nested arrays [index, [session, projnames]]
                session_proj = pd.Series([q[1].unique() for q in sproj])
                
                
            sessions_df['session_start'] = [q for q in pd.Series(session_starts)]
            sessions_df['session_duration_hours'] = [q * ns2hours for q in pd.Series(dur_total)]
            # if you had an exception before the indexing will be off by 1 because the session_proj will be zero-indexed 
            # instead of session_indexed, so do it element by element
            sessions_df['projects'] = [q for q in session_proj] 



        
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
        
        
        # Now write to the time series file (so it'll be there next time), if needed
        if newtsneeded:
            user_class_timeseries.to_csv(usertsfile)
        
        # definitely write the stats to the stats file
        with open(filestr_base+".ssv", 'a') as outfile:
            # now write to outfile
            outfile.write("%s;%.0f;%.0f;%.0f;%.0f;%s;%s;%.6f;%.6f;%.0f;%.0f;%.6f;%.6f;%.6f;%.6f;%.6f;%.6f;%.6f;%.6f;%.6f;%.6f;%.6f;%.6f;%.0f;%.0f;%.0f;%.6f;%.0f;%s;%s;%s;%s\n" % (theanonid,n_class,n_sessions,n_proj,n_days,first_day,last_day,tdiff_firstlast_hours,dur_class_total,count_min,count_max,count_med,count_mean,dur_class_mean_overall,dur_class_median_overall,dur_ses_mean,dur_ses_median,dur_ses_min,dur_ses_max,mean_duration_first2,mean_duration_last2,mean_class_duration_first2,mean_class_duration_last2,nproj_session_min,nproj_session_max,nproj_session_med,nproj_session_mean,home_proj_count,home_proj_json,all_proj_json,all_counts_json,ses_count_json))
        print "Finished session calculations at ",datetime.datetime.now().strftime('%H:%M:%S.%f')
        print "----------------------------------------"
        
        
        
        # print the overall session counts
        sescount_all_file = '/Volumes/Brooke_SD/VOLCROWE/user_timeseries/anon/sessions/sessions_' + theanonid + '_all.csv'
        if not os.path.exists(sescount_all_file):
            sessions_df.to_csv(sescount_all_file)
        
        
        
        print_by_proj(user_class_timeseries, all_proj, theanonid, thezooid, theuser)            
    
if dbconn:
    the_cnx.close()
    
    
