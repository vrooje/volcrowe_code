#by collaborator request, this reformats the output files of e.g. sessions_allproj_byuser.py so that each user has multiple columns

import sys 
import pandas as pd
import numpy as np
import os

projects = ['galaxy_zoo', 'planet_hunter', 'serengeti', 'sea_floor', 'penguin']

file_base = 'user_stats_out_'
sessions_file_base = 'user_sessions/sessions_'
outfilebase = 'user_sessions_newformat_'

# do each project sequentially
for thisproject in projects:

    print "Starting project "+thisproject

    # read the stats file to get the anon ids and session counts
    projstatsfile = file_base + thisproject + '.ssv'
    projstats = pd.read_csv(projstatsfile, sep=';')
    
    # figure out how many rows we need (for more helpful indexing later)
    nsessions = pd.Series(np.arange(1,max(projstats.n_sessions)+1))

    # initialize an empty set with session count as index
    projsessions = [] # just be really really sure it's empty
    projsessions = pd.DataFrame(data=nsessions)
    projsessions.columns=['session']
    projsessions.set_index('session', inplace=True)
    
    # we don't really care about anything else in the file but a list of anon ids
    anon_ids = projstats.anon_id.tolist()
    
    # loop through anon ids 
    for thisanonid in anon_ids:
        
        # read the anon id session file for this project, if it exists
        idprojfile = sessions_file_base+thisanonid+'_'+thisproject+'.csv'
        if os.path.exists(idprojfile):
            idsessions = pd.read_csv(idprojfile)
            
            # index this by session as well
            idsessions.set_index('session', inplace=True)
            
            # columns in input file: session,count,session_start,session_duration_hours,project
            # We want these columns in the output file
            datecol = 'Date ('+thisanonid+')'
            durcol = 'Session Length ('+thisanonid+')'
            nclasscol = 'Classifications ('+thisanonid+')'

            # we don't need the super-specific date, just YYYY-MM-DD
            idsessions[datecol] = [q[:10] for q in idsessions.session_start]
            
            # now assign the columns to the overall sessions dataframe
            # Note: even though the lengths will be different, because we've indexed by session it should still work
            projsessions[datecol] = idsessions[datecol]
            projsessions[durcol] = idsessions['session_duration_hours']
            projsessions[nclasscol] = idsessions['count']
            
        else:
            # If this happens it means a sessions file is missing that shouldn't be
            print "Warning: could not find file "+idprojfile
            
    
    # now projsessions should be filled with the data we need in the format we need
    # well, the headers aren't exactly to spec but they're close
    outfile = outfilebase+thisproject+'.csv'
    print "Writing to "+outfile
    projsessions.to_csv(outfile)
            
            



