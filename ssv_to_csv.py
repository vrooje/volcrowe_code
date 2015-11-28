# this file is for converting SSV files to csv on a by-project basis


import sys, os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import datetime
import dateutil.parser
import csv
import json



all_poss_proj = 'galaxy_zoo galaxy_zoo_starburst solarstormwatch moon_zoo ancient_lives whales milky_way oldweather sea_floor cyclone_center bat_detective cancer_cells andromeda serengeti planet_four notes_from_nature spacewarp leaf worms plankton radio war_diary m83 wise sunspot condor asteroid kelp penguin higgs_hunter chicago planet_hunter illustratedlife chimp orchid wisconsin crater'.split()



replacements = {'[':'', ']':'', ';':','}

# first do something easy for the by-project files (which don't have any columns with commas except the session counts)
for thisproj in all_poss_proj:

    # note .csvv is not a typo; if you save as .csv and open in Excel it will get clever with anon_id and break some of them.
    # by using .csvv you can still get it to interpret as comma-delimited BUT force it to interpret anon_id as text.
    with open('/Volumes/Brooke_SD/VOLCROWE/user_stats_out_'+thisproj+'.ssv') as infile, open('/Volumes/Brooke_SD/VOLCROWE/user_stats_out_'+thisproj+'.csvv', 'w') as outfile:
        for line in infile:
            for src, target in replacements.iteritems():
                line = line.replace(src, target)
            outfile.write(line)
            
            

# now read the all-projects file, which does have other columns with commas, and save only the columns we care about

user_stats = pd.read_csv('/Volumes/Brooke_SD/VOLCROWE/user_stats_out_new.ssv',sep=';')

user_stats['nclass_by_session_new'] = [(q.replace('[','')).replace(']','') for q in user_stats.nclass_by_session]

user_stats_lim = user_stats['anon_id n_classifications n_sessions n_projects unique_days first_classification last_classification t_spent_classifying_hours mean_class_duration_hours median_class_duration_hours mean_session_length_hours median_session_length_hours min_session_length_hours max_session_length_hours mean_session_length_first2_hours mean_session_length_last2_hours mean_class_duration_first2_hours mean_class_duration_last2_hours nclass_by_session_new'.split()]

user_stats_lim.to_csv('/Volumes/Brooke_SD/VOLCROWE/user_stats_out_new.csv')


            
