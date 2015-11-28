# this will make a ton of files that can be quickly read later, but it's not ideal so only use it in a pinch
# and if you don't mind having many many thousands of text files in a single directory or two

import sys
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


# timestamps & timediffs are in nanoseconds below but we want outputs in HOURS
# Note: I'd like to keep units in days but then a session length etc in seconds is ~1e-5 and that's too
#       close to floating-point errors for my liking (because this might be read into Excel)
# we will use either this below, or
# /datetime.timedelta(hours=1)
# depending on whether the output is in a timedelta (use above) or in float (use below).
ns2hours = 1.0 / (1.0e9*60.*60.)



# dbconfig = {
#   'user': 'root',
#   'password': '',
#   'host': '127.0.0.1',
#   'raise_on_warnings': True,
# }

#the_cnx = mysql.connector.connect(**dbconfig)
# the_cnx = pymysql.connect(user='root',passwd='',host='127.0.0.1')
# cursor = the_cnx.cursor()

get_all = 0


print "Getting users..."
if get_all:
    # this is for ALL users in the zooniverse
    #and is gonna take a loooooong time
    
    cursor = the_cnx.cursor()
    getusers = "select login as user_name, id as zooniverse_id, from zoohome_300315.users"
    cursor.execute(getusers)
    the_users = pd.DataFrame(cursor.fetchall())
    the_users.columns = cursor.column_names
    the_users['anon_id'] = [get_anon_id(q) for q in the_users.zooniverse_id]
    cursor.close()

else:
    user_file = '/Volumes/Brooke_SD/VOLCROWE/survey/survey_answered_ids_CONFIDENTIAL.csv'
    the_users = pd.read_csv(user_file)
    the_users.set_index('zooniverse_id', drop=False, inplace=True)


filestr_base = "/Volumes/Brooke_SD/VOLCROWE/user_stats_out"

# if you're not starting from scratch, this != 0
starting_row = 0

# if you only need to go up to a certain point
ending_row = 10000






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

    if (i < starting_row) | (i >= ending_row):
        print "Skipping user",theuser,"("+theanonid+")"
    else:
        #if i == len(the_users) % 20:
        print "User",i,"of",len(the_users),"("+theuser+", "+theanonid+")",datetime.datetime.now().strftime('%H:%M:%S.%f')
        
        
        
        # remove all non-alphanumeric characters from the username (so it doesn't break filenames)
        # no underscores allowed
        #pattern = re.compile('[\W_]+', re.UNICODE)
        # underscores ok, question marks ok
        #pattern = re.compile('\W+', re.UNICODE)
        # underscores ok but question marks not ok
        pattern = re.compile('[\W\xe9]+', re.UNICODE)
        theuserclean = pattern.sub('', theuser)
    
        usertsfile = '/Volumes/Brooke_SD/VOLCROWE/user_timeseries/timeseries_'+str(thezooid)+'_'+theuserclean+'.csv'
        userts_all = pd.read_csv(usertsfile)
        userts = userts_all['created_at_str project_name duration session'.split()]
        
        # we're re-reading old weather now that there are new classifications included
        # so first remove the existing OW classifications 
        
        useranontsfile = '/Volumes/Brooke_SD/VOLCROWE/user_timeseries/anon/timeseries_'+theanonid+'.csv'


        # rewrite the timeseries with everything included
        userts.to_csv(useranontsfile)
        
        #print theuserclean, theanonid   
    
