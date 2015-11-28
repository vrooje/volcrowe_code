# prints out a list of classification counts by user name for a given Zooniverse project output.
# written for pre-Panoptes projects; works with most of them.
import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import datetime
import dateutil.parser
import csv

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

path_class = '/Volumes/Brooke_SD/VOLCROWE/survey/'

csvsep = ","
tabsep = "\t"


createdat = 'created_at'

def classification_counts(project_name, classifications_file):

    print "Reading database", classifications_file, "for", project_name, ". . ."
    classifications_all = pd.read_csv(classifications_file)
    
    # the name of the user column varies depending on which project we're talking about.
    # even across Ouroboros projects it's not always the same.
        
    if 'user_name' in classifications_all.columns:
        username = 'user_name'
    elif 'user_id' in classifications_all.columns:
        username = 'user_id'
    elif 'user' in classifications_all.columns:
        username = 'user'
    elif 'zooniverse_user_id' in classifications_all.columns:
        username = 'zooniverse_user_id'
        
        
        

    # aggregate('count') on a groupby sometimes has a weird +/- 1 error, so make this column        
    classifications_all['count'] = ((classifications_all[createdat] < 1).astype(float))* 0.0 + 1.0
    
    
    users = classifications_all[username].unique()

    all_by_user = classifications_all.groupby(username)
    
    user_classification_count = all_by_user.count.aggregate(sum)



    outfile = path_class + project_name + 'user_classification_counts_all.csv'
    
    f = open(outfile, 'w')
    f.write('"user_name","num_classifications"\n')
    
    for the_user in users:
        f.write('\"%s\",' % the_user)
        f.write('\"%d\"\n' % int(user_classification_count[the_user]))

    f.close()


    outfile = path_class + project_name + 'user_classification_counts_registered.csv'
    f = open(outfile, 'w')
    f.write('"user_name","num_classifications"\n')
    
    for the_user in users:
        if not the_user.startswith("not-logged-in"):
            f.write('\"%s\",' % the_user)
            f.write('\"%d\"\n' % int(user_classification_count[the_user]))

    f.close()








    
    
















 