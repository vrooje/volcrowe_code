# this program is for once you've compiled user stats using sessions_allproj_byuser.py (which writes per-project 
# stats for each user in individual files) and want to make by-project stats files with one row per user
# note sessions_allproj_byuser.py makes a cross-project session stats file with one row per user. Just not per-project files.

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



# We are going to loop through users and check each project for the existence of a user stats file for that project.
# paths and project names come from sessions_allproj_byuser.py

all_poss_proj = 'galaxy_zoo galaxy_zoo_starburst solarstormwatch moon_zoo ancient_lives whales milky_way oldweather sea_floor cyclone_center bat_detective cancer_cells andromeda serengeti planet_four notes_from_nature spacewarp leaf worms plankton radio war_diary m83 wise sunspot condor asteroid kelp penguin higgs_hunter chicago planet_hunter illustratedlife chimp orchid wisconsin crater'.split()

# just make sure you're starting clean here (since this appends later)
for thisproj in all_poss_proj:
    execstr = 'rm /Volumes/Brooke_SD/VOLCROWE/user_stats_out' + '_' + thisproj + '.ssv'
    os.system(execstr)
    

# now start rebuilding the files
for i, thezooid in enumerate(the_users.zooniverse_id):
    theuser   = the_users[the_users.zooniverse_id==thezooid].user_name[thezooid]
    theanonid = the_users[the_users.zooniverse_id==thezooid].anon_id[thezooid]
    
    print "Starting per-project checks and additions for user",i,"of",len(the_users)," users:", theuser, theanonid, thezooid

    for thisproj in all_poss_proj:
        # if the user has stats on that project, add it (otherwise just skip)
        userssv_proj = '/Volumes/Brooke_SD/VOLCROWE/user_sessions/user_stats_out' + '_' + theanonid + '_' + thisproj + '.ssv'
        if os.path.exists(userssv_proj):
        
            thessv_proj = '/Volumes/Brooke_SD/VOLCROWE/user_stats_out' + '_' + thisproj + '.ssv'

            # if the file doesn't exist, start it and print a header
            if not os.path.exists(thessv_proj):
                with open(thessv_proj, 'w') as outfile:
                    
                    outfile.write("anon_id;n_classifications;n_sessions;unique_days;first_classification;last_classification;t_firstlast_hours;t_spent_classifying_hours;min_nclass_per_session;max_nclass_per_session;median_nclass_per_session;mean_nclass_per_session;mean_class_duration_hours;median_class_duration_hours;mean_session_length_hours;median_session_length_hours;min_session_length_hours;max_session_length_hours;mean_session_length_first2_hours;mean_session_length_last2_hours;mean_class_duration_first2_hours;mean_class_duration_last2_hours;nclass_by_session\n")

            
            # then just add the non-header line to the file
            execstr = "tail -n1 "+userssv_proj+" >> "+thessv_proj
            os.system(execstr)


