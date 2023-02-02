#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 1 14:44:24 2023

INFO
####

This script downloads Tweets from the full archive of Twitter using the academic
access API. It downloads geotagged tweets based on a list of bounding boxes.
The outputs are saved as a pickled dataframe (.pkl).

The bounding boxes can not be larger than 25 miles by 25miles or Twitter API
will not process the request. Matches against the place.geo.coordinates
object of the Tweet when present, and in Twitter, against a place geo polygon,
where the place polygon is fully contained within the defined region.

To construtct a GIS file out of them, please run the tweets_to_gpkg.py in the
folder containing the pickled dataframe files (.pkl files)

REQUIREMENTS
############

Files:
    .twitter_keys.yaml in the script directory
    premsearch_config.yaml in the script directory

Installed:
    Python 3.8 or newer
    
    Python packages:
        searchtweetsv2
        pandas
        geopandas

USAGE
#####

Run the script by typing:
    
    python v2_tweets_to_file.py -sd YEAR-MO-DA -ed YEAR-MO-DA

Replace YEAR with the year you want, MO with the month you want and DA with the
day of the month you want. For example:
    
    python v2_tweets_to_file.py -sd 2015-06-15 -ed 2019-06-15

NOTE
####

The collector collects tweets starting from 00:00 hours on the starting day and
ends the collection on 23:59:59 on the day before the end date. In the example
above the last collected day would be 2019-06-14.

The bounding box coordinates in the query require only two (2) coordinate pairs
first representing southwest corner of the bounding box, then northeast corner.
The CRS is basic WGS-84 in decimal degrees.

This script assumes you have created the bounding box with MMQGIS plugin in QGIS.


### SYDNEY SPECIFIC ###
No geotagged tweets before 01.09.2010

Collection ends February 1st 2023
12 years 5 months = 

@author: Tuomas Väisänen & Seija Sirkiä
"""

from util_functions import v2parser, daterange
from searchtweets import ResultStream, gen_request_parameters, load_credentials, read_config
from datetime import datetime, timedelta
import geopandas as gpd
import time
import argparse

# Set up the argument parser
ap = argparse.ArgumentParser()

# Get starting date
ap.add_argument("-sd", "--startdate", required=True,
                type=lambda s: datetime.strptime(s, '%Y-%m-%d'),
                help="Start date of the collection in the following form: "
                " YEAR-MO-DA for example 2018-01-28")

# Get end date
ap.add_argument("-ed", "--enddate", required=True,
                type=lambda s: datetime.strptime(s, '%Y-%m-%d'),
                help="End date of the collection in the following form: "
                " YEAR-MO-DA for example 2018-02-18")

# get wait time
ap.add_argument("-w", "--wait", required=False, default=45,
                help="Set wait time between requests to avoid Twitter rate limits. "
                "Default: 45")

# get interval
ap.add_argument("-in", "--interval", required=True, default=1,
                help="Set date intervals to avoid rate limits with popular areas. "
                "Default: 1, which is no intervals but everything all at once.")

# get bounding box geopackage
ap.add_argument("-b", "--bbox", required=True,
                help="Path to bounding box geopackage.")

# Parse arguments
args = vars(ap.parse_args())

# get waittime and interval
waittime = int(args['wait'])
interval = int(args['interval'])


# check if output filetypes are valid
if args['output'] == 'pkl':
    # save to pickle
    print('[INFO] - Output file set to pickle')
elif args['output'] == 'csv':
    # save to csv
    print('[INFO] - Output file set to csv')
else:
    print('[INFO] - Invalid output file! Valid options are pickle or csv. Exiting...')
    exit

# load bounding box
bbox_df = gpd.read_file(args['bbox'], driver='GPKG')

# get bbox order
bbox_df = bbox_df.assign(row_number=range(len(bbox_df)))

# get corner coordinates df

# load twitter keys
twitter_creds = load_credentials('.twitter_keys.yaml',
                               yaml_key = 'search_tweets_v2',
                               env_overwrite = False)

# load configuration for search query
search_config = read_config('search_config.yaml')

# fields for v2 api
tweetfields = ",".join(["attachments", "author_id", "conversation_id", "created_at",
                        "entities", "geo", "id", "in_reply_to_user_id", "lang",
                        "public_metrics", "possibly_sensitive", "referenced_tweets",
                        "reply_settings", "text", "withheld",])
userfields = ",".join(["created_at", "description", "entities", "location",
                       "name", "profile_image_url", "protected", "public_metrics",
                       "url", "username", "verified", "withheld"])
mediafields= ",".join(["media_key", "type", "url"])
placefields = ",".join(["contained_within", "country", "country_code", "full_name",
                        "geo", "id", "name",  "place_type"])
expansions = ",".join(["attachments.media_keys", "author_id", "entities.mentions.username",
                       "geo.place_id", "in_reply_to_user_id", "referenced_tweets.id",
                       "referenced_tweets.id.author_id"])

# get date interval
start_date = args['startdate'].date()
end_date = args['enddate'].date()

# get the amount of time per date intervals for looping
diff = (end_date - start_date) / interval

# loop over date intervals
for intv in range(interval):
    
    # get interval start date
    intstart = start_date + diff * intv
    
    # get interval end index
    intend_ix = intv + 1
    
    # get interval end date
    intend = start_date + diff * intend_ix
    
    # print message about which interval is collected
    print('[INFO] - Starting tweet collection between ' + str(intstart) + ' - ' + str(intend))
    
    # empty tweet list for current interval
    tweets_interval = []
    
    # loop over bounding boxes
    for i, bbox in bbox_df.iterrows():
        
        # extract southwest corner coordinate points
        west = bbox['left']
        south = bbox['bottom']
        
        # extract northeast corner coordinate points
        north = bbox['top']
        east = bbox['right']
        
        # form the search query based on bounding box southwest and northeast corner coordinates
        search_q = f'bounding_box:[{west:.5f} {south:.5f} {east:.5f} {north:.5f}]'
    
        # generate payload rules for v2 api
        rule = gen_request_parameters(query = search_q,
                                      results_per_call = search_config['results_per_call'],
                                      start_time = intstart.isoformat(),
                                      end_time = intend.isoformat(),
                                      tweet_fields = tweetfields,
                                      user_fields = userfields,
                                      media_fields = mediafields,
                                      place_fields = placefields,
                                      expansions = expansions,
                                      stringify = False)
        
        # initiate result stream from twitter v2 api
        rs = ResultStream(request_parameters = rule,
                          max_results=100000,
                          max_pages=1,
                          max_tweets = search_config['max_tweets'],
                          **twitter_creds)
    
        # number of reconnection tries
        tries = 10
        
        # while loop to protect against 104 error
        while True:
            tries -= 1
            
            # attempt retrieving tweets
            try:
                # indicate which day is getting retrieved
                print('[INFO] - Searching for tweets between ' + str(intstart) + ' and ' + str(intend) + ' from bounding box ' + str(i))
            
                # get json response to list
                tweets = list(rs.stream())
                
                # print response
                print('[INFO] - Got {} tweets from bounding box {}'.format(str(len(tweets)), str(i)))
                
                #  wait 6 seconds to avoid request bombing in case of zero or a few tweets
                time.sleep(6)
                
                # break free from while loop
                break
            
            # catch exceptions
            except Exception as err:
                if tries == 0:
                    raise err
                else:
                    print('[INFO] - Got connection error, waiting 15 seconds and trying again. ' + str(tries) + ' tries left.')
                    time.sleep(15)
        
        # extend current interval tweet list with tweets from current bounding box
        tweets_interval.extend(tweets)
        
    # check if there are results
    if len(tweets_interval) != 0:
    
        # parse results to dataframe
        print('[INFO] - Parsing collected tweets from ' + str(intstart) + ' to ' + str(intend))
        tweetdf = v2parser(tweets_interval, search_config['results_per_call'])
    
        # try to order columns semantically
        try:
            tweetdf = tweetdf[['id', 'author_id', 'created_at', 'conversation_id',
                               'in_reply_to_user_id', 'text', 'lang',
                               'public_metrics.retweet_count',
                               'public_metrics.reply_count', 'public_metrics.like_count',
                               'public_metrics.quote_count', 'user.location',
                               'user.created_at', 'user.username',
                               'user.public_metrics.followers_count',
                               'user.public_metrics.following_count',
                               'user.public_metrics.tweet_count',
                               'geo.place_id', 'geo.coordinates.type',
                               'geo.coordinates.coordinates',
                               'geo.coordinates.x', 'geo.coordinates.y', 'geo.full_name',
                               'geo.name', 'geo.place_type', 'geo.country',
                               'geo.country_code', 'geo.type', 'geo.bbox',
                               'geo.centroid', 'geo.centroid.x', 'geo.centroid.y']]
        except:
            
            pass
        
        # set up file prefix from config
        file_prefix_w_date = search_config['filename_prefix'] + '_' + str(start_date) + '---' + str(end_date)
        outpickle = file_prefix_w_date + '_part' + str(intv) + '.pkl'
        
        # save to pickle
        tweetdf.to_pickle('GIS/sydney/pickles/' + outpickle)
       
    else:
        # print message and move to next bbox
        print('[INFO] - No geotagged tweets in bounding boxes between {} and {}. Moving on...'.format(str(start_date), str(end_date)))
        pass

print('[INFO] - ... done!')