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

from collections import Counter
from collections import OrderedDict
from pymongo import MongoClient

try:
    from astropy.io import fits as pyfits
    from astropy.io.fits import Column
    from astropy.io import ascii
except ImportError:
    import pyfits
    from pyfits import Column


def choose_id(zooid, altid):
    try:
        thelen = len(zooid)
    except:
        thelen = zooid
        
    if thelen > 0:
        return zooid
    else:
        return altid


def ts_from_str(thetime):
    try:
        return pd.to_datetime(thetime, format='%Y-%m-%d %H:%M:%S.%f')
    except:
        try:
            return pd.to_datetime(thetime, format='%Y-%m-%d %H:%M:%S')
        except:
            return pd.to_datetime(thetime)


def stringify_zooid(zooid):
    try:
        if (np.isnan(zooid)):
            return ''
        else:
            return str(zooid)
    except:
        try:
            if (zooid == '\N') | (zooid == '\\N'):
                return ''
            else:
                return str(zooid)
        except:
            return str(zooid)

    return str(zooid)

# only considered registered users?
onlyregistered = True


path_class = './'

csvsep = ","
tabsep = "\t"

createdat = 'created_at'
username = 'user_id'


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





# create mega-list of databases and query types so we can do this all in one loop but also skip projects as needed

#[project_name, database_name.table_name, created_at_name, user_id_name, user_ip, do_these_stats]
#
# note user_id_name is just some logged-in ID, user_ip is some non-logged-in identifier.
# do_these_stats is 0 to skip, 1 to do the calculations.
allproj = [
['galaxy_zoo_1', 'zoohome_300315.gz1_classifications_basic', 'created_at', 'user_name', 'user_name',                        0],
['galaxy_zoo_2', 'GZ2_full.classifications', 'created_at', 'user_id', 'user_id',                                            0],
['galaxy_zoo_3', 'hubble_2014-10-05.classifications', 'created_at', 'zooniverse_user_id', 'zooniverse_user_id',             0],
["solarstormwatch", "SSW_20140914.classifications", 'created_at', 'zooniverse_user_id', 'zooniverse_user_id',               0],
["moon_zoo", "moonzoo_20140914.classifications", 'created_at', 'zooniverse_user_id', 'zooniverse_user_id',                  0],
["milky_way_1", "milkyway_20140914.classifications", 'created_at', 'zooniverse_user_id', 'zooniverse_user_id',              0],
["planet_hunter", "planethunters_20140914.classifications", 'created_at', 'zooniverse_user_id', 'session_id',               0],
["ancient_lives", "AncientLives_20140914.transcriptions_basic_withzooid", 'created_at', 'zooniverse_user_id', 'user_id',    0],
["whales", "whales_20140914.classifications_basic", 'created_at', 'zooniverse_user_id', 'whales_user_id',                   0],
["galaxy_zoo_4", "classifications_all_20150731.galaxy_zoo", "created_at_new", "zooniverse_id", 'user_ip',                   0],
["galaxy_zoo_starburst", "classifications_all_20150731.galaxy_zoo_starburst", "created_at_new", "zooniverse_id", 'user_ip', 0],
["milky_way_2", "classifications_all_20150731.milky_way", "created_at_new", "zooniverse_id", 'user_ip',                     0],
["oldweather", "classifications_all_20150731.oldweather", "created_at_new", "zooniverse_id", 'user_name',                   0],
["sea_floor", "classifications_all_20150731.sea_floor", "created_at_new", "zooniverse_id", 'user_ip',                       1],
["cyclone_center", "classifications_all_20150731.cyclone_center", "created_at_new", "zooniverse_id", 'user_ip',             0],
["bat_detective", "classifications_all_20150731.bat_detective", "created_at_new", "zooniverse_id", 'user_ip',               0],
["cancer_cells", "classifications_all_20150731.cancer_cells", "created_at_new", "zooniverse_id", 'user_ip',                 0],
["andromeda", "classifications_all_20150731.andromeda", "created_at_new", "zooniverse_id", 'user_ip',                       0],
["serengeti", "classifications_all_20150731.serengeti", "created_at_new", "zooniverse_id", 'user_ip',                       1],
["planet_four", "classifications_all_20150731.planet_four", "created_at_new", "zooniverse_id", 'user_ip',                   0],
["notes_from_nature", "classifications_all_20150731.notes_from_nature", "created_at_new", "zooniverse_id", 'user_ip',       0],
["spacewarp", "classifications_all_20150731.spacewarp", "created_at_new", "zooniverse_id", 'user_ip',                       0],
["leaf", "classifications_all_20150731.leaf", "created_at_new", "zooniverse_id", 'user_ip',                                 0],
["worms", "classifications_all_20150731.worms", "created_at_new", "zooniverse_id", 'user_ip',                               0],
["plankton", "classifications_all_20150731.plankton", "created_at_new", "zooniverse_id", 'user_ip',                         0],
["radio", "classifications_all_20150731.radio", "created_at_new", "zooniverse_id", 'user_ip',                               0],
["war_diary", "classifications_all_20150731.war_diary", "created_at_new", "zooniverse_id", 'user_ip',                       0],
["m83", "classifications_all_20150731.m83", "created_at_new", "zooniverse_id", 'user_ip',                                   0],
["wise", "classifications_all_20150731.wise", "created_at_new", "zooniverse_id", 'user_ip',                                 0],
["sunspot", "classifications_all_20150731.sunspot", "created_at_new", "zooniverse_id", 'user_ip',                           0],
["condor", "classifications_all_20150731.condor", "created_at_new", "zooniverse_id", 'user_ip',                             0],
["asteroid", "classifications_all_20150731.asteroid", "created_at_new", "zooniverse_id", 'user_ip',                         0],
["kelp", "classifications_all_20150731.kelp", "created_at_new", "zooniverse_id", 'user_ip',                                 0],
["penguin", "classifications_all_20150731.penguin", "created_at_new", "zooniverse_id", 'user_ip',                           1],
["higgs_hunter", "classifications_all_20150731.higgs_hunter", "created_at_new", "zooniverse_id", 'user_ip',                 0],
["chicago", "classifications_all_20150731.chicago", "created_at_new", "zooniverse_id", 'user_ip',                           0],
["planet_hunter_2", "classifications_all_20150731.planet_hunter", "created_at_new", "zooniverse_id", 'user_ip',             0],
["illustratedlife", "classifications_all_20150731.illustratedlife", "created_at_new", "zooniverse_id", 'user_ip',           0],
["chimp", "classifications_all_20150731.chimp", "created_at_new", "zooniverse_id", 'user_ip',                               0],
["orchid", "classifications_all_20150731.orchid", "created_at_new", "zooniverse_id", 'user_ip',                             0],
["wisconsin", "classifications_all_20150731.wisconsin", "created_at_new", "zooniverse_id", 'user_ip',                       0],
["crater", "classifications_all_20150731.crater", "created_at_new", "zooniverse_id", 'user_ip',                             0]
]


for theproject in allproj:

    readsomething = True #initially assume it's going to work
    project_name = theproject[0]

    # if this isn't marked to skip
    if theproject[5] > 0:
        thedb = theproject[1]
        thecreatedat = theproject[2]
        theid = theproject[3]
        theip = theproject[4]
        
        
        classifications_all = qq = user_class = []
        
        # it is much faster to read a csv file than to query the database, so try that first
        try:

            classfile_in = 'classification_files/'+project_name+'_class_basic_out.csv'
            print " Trying to read from", classfile_in
            qq = pd.read_csv(classfile_in)
            print " Read from", classfile_in
            
            # still need to convert to datetime even if we read from a file
            print "   ....starting timeseries... "
            try:
                qq['created_at'] = [pd.to_datetime(q, format='%Y-%m-%d %H:%M:%S.%f') for q in qq.created_at_str]
            except ValueError:
                try:
                    qq['created_at'] = [pd.to_datetime(q, format='%Y-%m-%d %H:%M:%S') for q in qq.created_at_str]
                except ValueError:
                    qq['created_at'] = [pd.to_datetime(q) for q in qq.created_at_str]
                    
            # making the assumption that the file is already sorted by created_at
            if onlyregistered:
                print "   Removing unregistered users in",project_name,"..." 
                qq['zooid_str'] = [stringify_zooid(q) for q in qq.zooid]
                isregistered = qq.zooid_str.str.len() > 1
                classifications_all = qq[isregistered]
            else:
                classifications_all = qq
            
            # it exists, so don't write it again below
            writethecsv = 0
        except Exception as inst:
            print " OOPS, that didn't work, because:"
            print inst 
            print " \n\n\n Moving On \n\n\n"
            try:
                classfile_in = 'classification_files/'+project_name+'.csv'
                print " Trying to read from", classfile_in
                qq = pd.read_csv(classfile_in)
                print "   .... read, checking for bad rows... "
                okdate_probably = qq.created_at_str.str.startswith('20')
                nbadrows = np.sum(np.invert(okdate_probably))
                
                if (nbadrows > 0):
                    print "          ***********", nbadrows, "rows misformatted, skipping, but ruh-roh"
                    qq = qq[okdate_probably]
                print "   .... done, starting timeseries... "
                # still need to convert to datetime even if we read from a file
                try:
                    qq['created_at'] = [pd.to_datetime(q, format='%Y-%m-%d %H:%M:%S.%f') for q in qq.created_at_str]
                except ValueError:
                    try:
                        qq['created_at'] = [pd.to_datetime(q, format='%Y-%m-%d %H:%M:%S') for q in qq.created_at_str]
                    except ValueError:
                        qq['created_at'] = [pd.to_datetime(q) for q in qq.created_at_str]
                        
                # making the assumption that the file is already sorted by created_at
                if onlyregistered:
                    print "   Removing unregistered users in",project_name,"..."
                    qq['zooid_str'] = [stringify_zooid(q) for q in qq.zooid]
                    isregistered = qq.zooid_str.str.len() > 1
                    classifications_all = qq[isregistered]
                else:
                    classifications_all = qq
                
                # this file isn't as complete as the other one, so we still have to write the file below
                writethecsv = 1
                
            except:   # if we can't read either file we'll have to deal directly with the database
                print " OOPS, that didn't work, because:"
                print inst 
                print " \n\n\n Moving On \n\n\n"

                try:
                    print "Starting query for "+project_name, datetime.datetime.now().strftime('%H:%M:%S.%f')
                
                    cursor = the_cnx.cursor()
                    writethecsv = 1
                    
                    query = "select convert(replace("+thecreatedat+", '\"', '') using latin1) as created_at_str, "+theid+" as "+username+", "+theip+" as alt_id, '"+project_name+"' as project_name from "+thedb +" order by created_at_str " #+" limit 1000"
                    #print query
                    cursor.execute(query)
                    aa = cursor.fetchall()
                    cursor.close()
                    #qq = pd.DataFrame(aa)
                    if len(aa) > 0:
                        qq = pd.DataFrame(np.array(aa),columns='created_at_str zooid altid project_name'.split())
                        #qq.columns = cursor.column_names
                        #qq['created_at'] = [ts_from_str(q) for q in qq.created_at_str]
                        
                        print "      ensuring we skip badly formatted rows. . .", datetime.datetime.now().strftime('%H:%M:%S')
                        okdate_probably = qq.created_at_str.str.startswith('20')
                        nbadrows = np.sum(np.invert(okdate_probably))
                        
                        if (nbadrows > 0):
                            print "          ***********", nbadrows, "rows misformatted, skipping, but ruh-roh"
                            qq = qq[okdate_probably]
                        
                        print "      converting date strings to datetime. . . ",datetime.datetime.now().strftime('%H:%M:%S')
                        
                        # when I thought having a variable format was the problem
                        #qq['created_at_strc'] = [q[:19] for q in qq.created_at_str]
                        
                        try:
                            qq['created_at'] = [pd.to_datetime(q, format='%Y-%m-%d %H:%M:%S.%f') for q in qq.created_at_str]
                        except ValueError:
                            try:
                                qq['created_at'] = [pd.to_datetime(q, format='%Y-%m-%d %H:%M:%S') for q in qq.created_at_str]
                            except ValueError:
                                qq['created_at'] = [pd.to_datetime(q) for q in qq.created_at_str]
                
                        user_class = qq.copy()
    
                        if onlyregistered:
                            print "   Removing unregistered users in",project_name,"..."
                            try:
                                qq['zooid_str'] = [stringify_zooid(q) for q in qq.zooid]
                                isregistered = qq.zooid_str.str.len() > 1
                                classifications_all = qq[isregistered].sort('created_at', ascending=True)
                            except: 
                                classifications_all = qq[isregistered]
                        else:
                            try:
                                classifications_all = qq.sort('created_at', ascending=True)
                            except: 
                                classifications_all = qq

                except Exception as inst:
                    print "You're really not having much luck (the query part didn't work either). The error:"
                    print inst
                    print "\n\n\n       At this point, I'm giving up and moving on..."
                    readsomething=False
                    
        if readsomething:
                       
            # Now we've read the files/made the query and loaded classifications_all with the data
            
            if len(classifications_all) > 1:
                
                print "     starting calculations. . .",datetime.datetime.now().strftime('%H:%M:%S.%f')
                
                if not ('user_id' in classifications_all.columns):
                    classifications_all['user_id'] = [choose_id(q[1]['zooid'], q[1]['altid']) for q in classifications_all.iterrows()]
                
                if not ('class_day' in classifications_all.columns):
                    classifications_all['class_day'] = classifications_all['created_at_str'].apply(lambda x:x[:10])
        
                date_temp = classifications_all['created_at_str'].copy()
                
                # first day, last day -- only the day
                start_date = min(date_temp)[:10]
                end_date = max(date_temp)[:10]
                
                start_dt = pd.to_datetime(start_date)
                end_dt = pd.to_datetime(end_date)
                
                #23:59:59 - to include the full day, but not 00:00:00 of the next, when selecting on this_dt below
                timestep = datetime.timedelta(days=1)-datetime.timedelta(seconds=1) 
                
                
                # and index by datetime - does this make it a TimeSeries? A: yes, if the column is formatted as datetime.
                classifications_all.set_index('created_at', inplace=True, drop=False)
                
                classifications_all['count'] = ((classifications_all.created_at_str < 1).astype(int)) * 0.0 + 1.0
                
                #classifications_all['duration'] = (classifications_all.created_at_str < 1).astype(float)
                #classifications_all['duration'] *= 0.0
                
                # there are variations among projects in the labels of the following things we need:
                # subject_id:  aka image_name || filename || image_id || subject_zooniverse_id
                # user_name:   aka user_id || user
                # created_at:     so far this is unique
                # started_at:  may not exist at all
                
                
                # We are defining a "classification" as a completed workflow (i.e. a set of markings or questions) for a subject.
                # But the projects handle classification outputs a little differently between them.
                # Galaxy Zoo, for example, is consistent with our definition. All the answers in the workflow are in one row in the csv.
                # In Penguin Watch, however, each time a user marks a penguin that counts as a classification, regardless of what subject is shown.
                #     (At least in the classifications csv dump.) Serengeti is the same way.
                # We only want to count that whole group as one classification, so we need to group classifications by user and subject.
                # And since we don't care what the content of the classification was we can just take each user,subject combo as a classification.
                # For projects like GZ where the subj-user pair is already unique (ignoring bugs, which I think are very rare) this won't change anything.
                # However, if we know that to be true then maybe just skip this step as it can take a lot of time/memory
                # Also note: for Sunspotters, a pairwise comparison project, it's always 2 subjects per classification, so the unique combo changes
                
                classifications_use = classifications_all
                
                pn = project_name.lower()
                
                #print project_name + ": Making sure classifications are consistently defined . . ."
                #if pn[:6] == 'galaxy' or pn[:2] == 'gz' or pn[:7] = 'sunspot':
                #    classifications_use = classifications_all
                #else:
                #    subj_classifications = classifications_all.groupby([username, subjectid])
                #    classifications_use = subj_classifications.head(1)
                
                
                
                # Are we going to need a loop? 
                # NO we should be able to set a date column with just year-month-day and then groupby and then apply all these stats... I think.
                print "  " + project_name + ": Grouping by day and aggregating . . ."
                by_day = classifications_use.groupby('class_day')
                the_days = classifications_use.class_day.unique()
                
                the_stats = pd.DataFrame(the_days)
                the_stats.columns=['the_day']
                the_stats = the_stats.set_index('the_day', drop=True)
                
                # get column of days since project start (for easier cross-project comparison)
                #the_stats['tvalue'] = pd.to_datetime(the_stats.index)
                the_stats['tvalue'] = [pd.to_datetime(q) for q in the_stats.index]
                start_date_ts = the_stats['tvalue'][start_date]
                try:
                    the_stats['days_since_start'] = (the_stats['tvalue'] - start_date_ts)/np.timedelta64(1, 'D')
                except:
                    the_stats['days_since_start'] = (the_stats['tvalue'] < 1).astype(float)
                the_stats.drop('tvalue', 1, inplace=True) # we don't need this to print as it's already in the index
                
                
                
                
                #n_classifications, active_registered_users, active_unregistered_users, number of classifications from registered users
                class_counts_day = by_day.count.aggregate(sum)
                the_stats['class_count_today'] = class_counts_day
                
                # so we can easily plot projects of different sizes against each other
                the_stats['class_count_normalised'] = the_stats['class_count_today']/float(max(the_stats['class_count_today']))
                # placeholder; defining the column now so the csv prints in the order I want
                the_stats['class_count_cumulative'] = class_counts_day
                
                # count number of users who classified on a given day
                users_list_day = by_day['user_id'].unique() # this makes a list of user ids for each day
                users_by_day = users_list_day.apply(lambda x:len(x)) # which we can then count the length of
                the_stats['active_user_count'] = users_by_day # and then put it into our dataframe
                
                # this is easy to calculate now so we might as well
                the_stats['active_user_count_normalised'] = the_stats['active_user_count']/float(max(the_stats['active_user_count']))
                # placeholder; defining the column now so the csv prints in the order I want
                the_stats['unique_users_cumulative'] = users_by_day
                
                
                # figure out when each user joined the project
                # assumes the classifications file is in chronological order so .head(1) picks their first classification date
                # then reindex so we have a timeseries. Seems like a lot of code for something kinda simple
                # Worth it to do this, though, as then the slice below can be much smaller + not need a .unique() --> use less memory+time in the loop
                user_joined_at = classifications_all[['user_id', createdat]].groupby('user_id').head(1)
                user_joined_at.reset_index(drop=True, inplace=True)
                try:
                    user_joined_at[createdat] = pd.to_datetime(user_joined_at[createdat], format='%Y-%m-%d %H:%M:%S %Z')
                except ValueError:
                    user_joined_at[createdat] = pd.to_datetime(user_joined_at[createdat], format='%Y-%m-%d %H:%M:%S')
                user_joined_at.set_index(createdat, inplace=True)
                
                
                
                for this_day in the_days:
                    if this_day[8:10] == '01':
                        print '   '+project_name+' Cumulative through '+this_day+' (towards '+end_date+') . . .'
                    this_dt = pd.to_datetime(this_day)
                    end_of_this_dt = this_dt+timestep
                    #+timestep includes all of this day  <-- because using a datetime is exact & this_day implies 00:00:00. 
                    #up_thru_this = classifications_all.truncate(after=this_day) 
                    try:    
                        # this is crashing unless there's an exact match b/c it's a datetime and not a string
                        # http://pandas.pydata.org/pandas-docs/stable/timeseries.html
                        # But this is faster when it works, so when it works, use it
                        #up_thru_this = classifications_all['user_id'][:end_of_this_dt]
                        the_stats.class_count_cumulative[this_day] = len(classifications_all['user_id'][:end_of_this_dt])
                        the_stats.unique_users_cumulative[this_day] = len(user_joined_at['user_id'][:end_of_this_dt])     
                    except KeyError as e:
                        #print KeyError, e, pd.to_datetime(e.message), ', using slower method...'
                        # note this will include anything up to 23:59:59 on the day so we don't need to add the timestep
                        #up_thru_this = classifications_all['user_id'][:this_day]
                        the_stats.class_count_cumulative[this_day] = len(classifications_all['user_id'][:this_day])
                        the_stats.unique_users_cumulative[this_day] = len(user_joined_at['user_id'][:this_day])     
                        
                    #the_stats.class_count_cumulative[this_day] = len(up_thru_this)
                    #the_stats.unique_users_cumulative[this_day] = len(up_thru_this.unique())
                
                print '    . . . cumulative stats finished.'
                
                # rewrite the stats csv no matter what            
                file_out = 'dailystats/dailystats_' + project_name+'.csv'
                print project_name + ": Writing to "+file_out
                the_stats.to_csv(file_out)
                
                #only write the project classification file if it doesn't already exist
                if writethecsv == 1:
                    classfile_out = 'classification_files/'+project_name+'_class_basic_out.csv'
                    print "      writing to", classfile_out
                    classifications_all.to_csv(classfile_out)
         
         
            else:
                print "Classification file or array empty, so nothing being done."
        else:
            print "Nothing was read, so nothing was done."
    else:
        print "Skipping "+project_name+" as requested..."
   
    try:
        cursor.close()
    except:
        pass
    
    
    
    
        
    
    
