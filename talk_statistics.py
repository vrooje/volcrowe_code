# this is an older bit of code and needs updating for optimisation

import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import datetime
import dateutil.parser
import csv

from ast import literal_eval
from collections import Counter
from collections import OrderedDict
from pymongo import MongoClient
from anon_id_utils import get_anon_id

try:
    from astropy.io import fits as pyfits
    from astropy.io.fits import Column
    from astropy.io import ascii
except ImportError:
    import pyfits
    from pyfits import Column

path_class = './'

csvsep = ","
tabsep = "\t"

# TO DO: Split MWP into 1 and 2

projects = ["penguin", "sea_floor", "serengeti", "galaxy_zoo", "planet_hunters"]

for project_name in projects:

    discussions_filename = path_class + "classification_files/" + project_name + "_discussions.csv"

    print "Reading database", discussions_filename, "for", project_name, ". . ."
    discussions_all = pd.read_csv(discussions_filename, low_memory=False, warn_bad_lines=True, error_bad_lines=False, dtype={'tags':'S'})
    
    
    username = 'none'
    if 'user_name' in discussions_all.columns:
        username = 'user_name'
    elif 'user_id' in discussions_all.columns:
        username = 'user_id'
    elif 'user' in discussions_all.columns:
        username = 'user'
    elif 'zooniverse_user_id' in discussions_all.columns:
        username = 'zooniverse_user_id'
        
        
    userid = 'none'
    if 'zooniverse_user_id' in discussions_all.columns:
        userid = 'zooniverse_user_id'
    elif 'user_zooniverse_id' in discussions_all.columns:
        userid = 'user_zooniverse_id'
        
    # contains the actual text of the discussions    
    body = 'none'
    if 'body' in discussions_all.columns:
        body = 'body'
    elif '_body' in discussions_all.columns:
        body = '_body'
        
    # if this isn't a column then we have to extract it from the body column
    tags = 'none'
    if 'tags' in discussions_all.columns:
        tags = 'tags'
    elif '_tags' in discussions_all.columns:
        tags = '_tags'

    
    anon_id = 'anon_id'
    if not anon_id in discussions_all.columns:
        print project_name,"Warning: anon_id wasn't defined, defining from ",userid," i.e. numeric Zooniverse user ID"
        discussions_all['anon_id'] = (discussions_all[userid] < 1).astype(str)
        discussions_all.anon_id = [get_anon_id(q) for q in discussions_all[userid]]

        
    
        
    createdat = 'created_at'
            
    # not sure what happens if this isn't a list, as it isn't with the ouroboros talk projects
    if body != 'none' and (project_name == 'sea_floor' or project_name == 'planet_hunters'):
        discussions_all['body_arr'] = discussions_all[body].apply(literal_eval)
    elif body != 'none':
        discussions_all['body_arr'] = discussions_all[body].astype(str)
        discussions_all['body_arr'] = discussions_all['body_arr'].apply(lambda x:x.split(' '))
    else:
        discussions_all['body_arr'] = ['' for q in discussions_all.anon_id]
        
    
    discussions_all['word_count'] = discussions_all.body_arr.apply(lambda x:len(x))
    
    if tags == 'none':
        if body == 'none':
            discussions_all['tag_count'] = discussions_all[userid] * 0
        else:
            discussions_all['tag_body'] = discussions_all[body].astype(str)
            discussions_all['tag_count'] = discussions_all['tag_body'].apply(lambda x:x.count("#"))
    else:
        discussions_all[tags] = discussions_all[tags].astype(str)
        try:
            discussions_all['tag_arr'] = discussions_all[tags].apply(literal_eval)
            discussions_all['tag_count'] = discussions_all.tag_arr.apply(lambda x:len(x))
        except ValueError:
            #discussions_all['tag_arr'] = discussions_all[tags].apply(lambda x:x.split(';'))
            discussions_all['tag_count'] = discussions_all[tags].apply(lambda x:count_tags(x))

    discussions_all['count'] = (discussions_all[createdat] < 1).astype(float)
    discussions_all['count'] *= 0.0
    discussions_all['count'] += 1
    
    #discussions_all['duration'] = (discussions_all[createdat] < 1).astype(float)
    #discussions_all['duration'] *= 0.0
    
    # there are variations among projects in the labels of the following things we need:
    # subject_id:  aka image_name || filename || image_id || subject_zooniverse_id
    # user_name:   aka user_id || user
    # created_at:     so far this is unique
    # started_at:  may not exist at all

    by_user = discussions_all.groupby(anon_id)
    
    
    #print "Grouping by user . . . "    
    #by_user = discussions[[username, createdat, subjectid, "count", "duration"]].groupby(discussions[username])
    #print "Grouping by subject . . . "    
    #by_subject = discussions[[username, createdat, subjectid, "count", "duration"]].groupby(discussions[subjectid])
    #subject_discussion_count = by_subject.count.aggregate(sum)
    

    # q: is the duration of a discussion the difference between started_at and created_at?
    # maybe the absolute value -- one column is not always larger/later than the other, weirdly.
    # can take the difference between the first and last started_at for typical time a user spends
    # on a project.
    discussions = discussions_all
    n_discussions = len(discussions)

    #the_subjects = discussions[subjectid].unique()
    the_users = discussions[anon_id].unique()
    n_users = len(the_users)
            
 

    print project_name,": Getting user activity durations . . ."
    # user_active is in hours
    user_active = by_user.created_at.apply(lambda x:(abs(dateutil.parser.parse(x.iloc[-1]) - dateutil.parser.parse(x.iloc[0])).days*24. + abs(dateutil.parser.parse(x.iloc[-1]) - dateutil.parser.parse(x.iloc[0])).seconds/(60.*60.))/24.)

    print project_name,": Getting user statistics . . ."    
    user_discussion_count = by_user.count.aggregate(sum)
    user_word_tot = by_user.word_count.aggregate(sum)  
    user_tag_tot  = by_user.tag_count.aggregate(sum)
    



    filestr_base = path_class + "classification_files/" + project_name + "_discussions_stats"

    with open(filestr_base+".csv", 'w') as outfile:
        outfile.write("anon_id,n_talk_posts,duration_firstlast_days,n_talk_words,n_hashtags\n")
        for i_user, this_user in enumerate(the_users):
            outfile.write("%s,%.0f,%.5f,%.0f,%.0f\n" % (this_user, user_discussion_count[this_user], user_active[this_user], user_word_tot[this_user], user_tag_tot[this_user]))
    
    
    
    
def count_tags(x):
    # because we forced type string NaN got changed to "nan"
    if len(x) > 1 and x != "nan":
        return x.count(";")+1
    else:
        return 0    
    
    
    
def datediff_from_iso(created_at, time_start):
    #hi - this function is so that above, the bulky line
    #discussions[thecols].apply(lambda x: abs(dateutil.parser.parse(x[createdat]) - dateutil.parser.parse(x[timestart]+" UTC")).days*24. + abs(dateutil.parser.parse(x[createdat]) - dateutil.parser.parse(x[timestart]+" UTC")).seconds/3600., axis=1)
    # becomes 
    #discussions[thecols].apply(lambda x: datediff_from_iso(x[createdat], x[timestart]), axis=1)
    datediff = abs(dateutil.parser.parse(created_at) - dateutil.parser.parse(time_start+" UTC"))
    return datediff.days*24. + datediff.seconds/3600.
    
    
    
    
def gini(list_of_values):
    sorted_list = sorted(list_of_values)
    height, area = 0, 0
    for value in sorted_list:
        height += value
        area += height - value / 2.
    fair_area = height * len(list_of_values) / 2
    return (fair_area - area) / fair_area
    
    
    
def talk_stats_v1(project_name, sellers_directory):
    stub_filename = sellers_directory+"/talk_"+project_name
    comments_filename = stub_filename+"_comments_2014-09-21.csv"
    discussions_filename = stub_filename+"_discussions_2014-09-21.csv"
    users_filename = stub_filename+"_users_2014-09-21.csv"
    
    comments = pd.read_csv(comments_filename)
    discussions = pd.read_csv(discussions_filename)
    users = pd.read_csv(users_filename)
    
    n_users_visited = len(users['_id'].unique())
    n_users_posted  = len(comments['author_id'].unique())
    # there is a discussion id for every subject in Talk v1 whether or not anyone has posted on it.
    n_discussions = len(discussions[discussions['number_of_comments'] > 0])
    n_conversations = len(discussions[discussions['number_of_comments'] > 1])
    n_comments = len(comments['_id'])
    
    print project_name
    print n_comments,"\t",n_discussions,"\t",n_users_visited,"\t",float(n_users_posted)/float(n_users_visited),"\t",float(n_conversations)/float(n_discussions)





def talk_stats_v2(project_name, filename_discussions):
    discussions = pd.read_csv(filename_discussions)
    n_users_visited = len(discussions['user_zooniverse_id'].unique())
    n_users_posted = len(discussions['user_zooniverse_id'].unique())
    n_discussions = len(discussions['discussion_id'].unique())
    n_comments = len(discussions['comment_id'].unique())
    by_thread = discussions.groupby(discussions['discussion_id'])
    
    discussion_lengths = by_thread.comment_id.agg('count')
    
    conversations = discussion_lengths > 1
    n_conversations = len(discussion_lengths[conversations])
    
    print project_name
    print n_comments,"\t",n_discussions,"\t",n_users_visited,"\t",float(n_users_posted)/float(n_users_visited),"\t",float(n_conversations)/float(n_discussions)
    
    
    
    
    
def run_all():
    
    data_in_dir = '/Volumes/BDS_backup/VOLCROWE/'
    
    discussion_statistics('AncientLives', '/Volumes/BDS_backup/VOLCROWE/2014-09-14_ancientlives_discussions_queryresult.csv')
    discussion_statistics('AndromedaProject', '/Volumes/BDS_backup/VOLCROWE/2014-09-21_andromeda_discussions.csv')
    discussion_statistics('BatDetective', '/Volumes/BDS_backup/VOLCROWE/2014-09-21_bat_detective_discussions.csv')
    discussion_statistics('CellSlider', '/Volumes/BDS_backup/VOLCROWE/2014-09-21_cancer_cells_discussions.csv')
    discussion_statistics('CycloneCenter', '/Volumes/BDS_backup/VOLCROWE/2014-09-21_cyclone_center_discussions.csv')
    discussion_statistics('GalaxyZoo1', '/Volumes/BDS_backup/VOLCROWE/gz1_discussions_queryresult_withheaders.csv')
    discussion_statistics('GalaxyZoo2', '/Volumes/BDS_backup/VOLCROWE/2013-07-05_gz2_discussions_queryresult.csv')
    discussion_statistics('GalaxyZoo3', '/Volumes/BDS_backup/VOLCROWE/2012-10-05_gz3_discussions_queryresult.csv')
    discussion_statistics('GalaxyZoo4', '/Volumes/BDS_backup/VOLCROWE/2014-09-21_galaxy_zoo_discussions_basiconly.csv')
    discussion_statistics('MilkyWay1', '/Volumes/BDS_backup/VOLCROWE/2014-09-14_milkyway_discussions_queryresult_v1_only.csv')
    discussion_statistics('MilkyWay2', '/Volumes/BDS_backup/VOLCROWE/2014-09-14_milkyway_discussions_queryresult_v2_only.csv')
    discussion_statistics('MoonZoo', '/Volumes/BDS_backup/VOLCROWE/2014-09-14_moonzoo_discussions_query_result.csv')
    discussion_statistics('OldWeather','/Volumes/BDS_backup/VOLCROWE/oldweather_transcriptions_20140930_nousersNaN.csv')
    discussion_statistics('PlanetFour', '/Volumes/BDS_backup/VOLCROWE/2014-09-21_planet_four_discussions_basiconly.csv')
    discussion_statistics('PlanetHunters', '/Volumes/BDS_backup/VOLCROWE/2014-09-14_planethunters_discussions_queryresult.csv')
    discussion_statistics('SnapshotSerengeti', '/Volumes/BDS_backup/VOLCROWE/2014-09-21_serengeti_discussions_basiconly_noweirdrows.csv')
    discussion_statistics('SolarStormWatch', '/Volumes/BDS_backup/VOLCROWE/2014-09-14_SSW_discussions_query.csv')
    discussion_statistics('WhaleFM', '/Volumes/BDS_backup/VOLCROWE/2014-09-21_whales_query_result.csv')
    discussion_statistics('SeaFloorExplorer', '/Volumes/BDS_backup/VOLCROWE/2014-09-24_sea_floor_discussions.csv')
    
    data_out_dir = '/Users/kathyfoley/Desktop/'
    suffix = '.cat'
    
    for suffix in '.dat .csv'.split(' '):
        #cp -f ~/Desktop/Ancientlives.cat ~/Desktop/allproj.cat
        #tail -n1 ~/Desktop/AndromedaProject.cat >> ~/Desktop/allproj.cat
        # etc.
        tablestr  = "cp -f "+data_out_dir+"AncientLives"+suffix+" "+data_out_dir+"allproj"+suffix+"\n"
        tablestr += "tail -n1 "+data_out_dir+"AndromedaProject"+suffix+" >> "+data_out_dir+"allproj"+suffix+"\n"
        tablestr += "tail -n1 "+data_out_dir+"BatDetective"+suffix+" >> "+data_out_dir+"allproj"+suffix+"\n"
        tablestr += "tail -n1 "+data_out_dir+"CellSlider"+suffix+" >> "+data_out_dir+"allproj"+suffix+"\n"
        tablestr += "tail -n1 "+data_out_dir+"CycloneCenter"+suffix+" >> "+data_out_dir+"allproj"+suffix+"\n"
        tablestr += "tail -n1 "+data_out_dir+"GalaxyZoo1"+suffix+" >> "+data_out_dir+"allproj"+suffix+"\n"
        tablestr += "tail -n1 "+data_out_dir+"GalaxyZoo2"+suffix+" >> "+data_out_dir+"allproj"+suffix+"\n"
        tablestr += "tail -n1 "+data_out_dir+"GalaxyZoo3"+suffix+" >> "+data_out_dir+"allproj"+suffix+"\n"
        tablestr += "tail -n1 "+data_out_dir+"GalaxyZoo4"+suffix+" >> "+data_out_dir+"allproj"+suffix+"\n"
        tablestr += "tail -n1 "+data_out_dir+"MilkyWay1"+suffix+" >> "+data_out_dir+"allproj"+suffix+"\n"
        tablestr += "tail -n1 "+data_out_dir+"MilkyWay2"+suffix+" >> "+data_out_dir+"allproj"+suffix+"\n"
        tablestr += "tail -n1 "+data_out_dir+"MoonZoo"+suffix+" >> "+data_out_dir+"allproj"+suffix+"\n"
        tablestr += "tail -n1 "+data_out_dir+"PlanetFour"+suffix+" >> "+data_out_dir+"allproj"+suffix+"\n"
        tablestr += "tail -n1 "+data_out_dir+"PlanetHunters"+suffix+" >> "+data_out_dir+"allproj"+suffix+"\n"
        tablestr += "tail -n1 "+data_out_dir+"SnapshotSerengeti"+suffix+" >> "+data_out_dir+"allproj"+suffix+"\n"
        tablestr += "tail -n1 "+data_out_dir+"SolarStormWatch"+suffix+" >> "+data_out_dir+"allproj"+suffix+"\n"
        tablestr += "tail -n1 "+data_out_dir+"WhaleFM"+suffix+" >> "+data_out_dir+"allproj"+suffix+"\n"
        print tablestr
    
        exec(tablestr)
    
    
    
    
if __name__ == '__main__':


    # name of the project
    try:
        sys.argv[1]
    except IndexError:
        project_name = 'GZ4'
    else:
        project_name = sys.argv[1]

    # file with raw discussions (csv)
    try:
        sys.argv[2]
    except IndexError:
        discussions_filename = path_class + '2014-09-21_galaxy_zoo_discussions.csv'
    else:
        discussions_filename = sys.argv[2]


    discussion_statistics(project_name, discussions_filename)
    #assign_weights_from_training(sys.argv[1], sys.argv[2], sys.argv[3])
