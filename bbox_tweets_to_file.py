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

# get save format
ap.add_argument("-o", "--output", required=True, default='pkl',
                help="Output file format, valid options are either pkl or csv. "
                "Default: pkl")

# get retrieval style
ap.add_argument("-s", "--style", required=True, default='iterative',
                help="Set retrieve style. Options: bulk or iterative. Bulk collects "
                "tweets to one big file. Iterative collects each day separately")

# get wait time
ap.add_argument("-w", "--wait", required=False, default=45,
                help="Set wait time between requests to avoid Twitter rate limits. "
                "Default: 45")

# get bounding box geopackage
ap.add_argument("-b", "--bbox", required=True,
                help="Path to bounding box geopackage.")

# Parse arguments
args = vars(ap.parse_args())

# get waittime
waittime = int(args['wait'])

# get retrieval style
rstyle = args['style']

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

# set interval to loop through
start_date = args['startdate'].date()
end_date = args['enddate'].date()

for bbox in bbox_df.iterrows():
    
    # extract southwest corner coordinate points
    
    # extract northeast corner coordinate points
    
    

# payload rules for v2 api
rule = gen_request_parameters(query = search_config['query'],
                              results_per_call = search_config['results_per_call'],
                              start_time = start_date.isoformat(),
                              end_time = end_date.isoformat(),
                              tweet_fields = tweetfields,
                              user_fields = userfields,
                              media_fields = mediafields,
                              place_fields = placefields,
                              expansions = expansions,
                              stringify = False)

# result stream from twitter v2 api
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
        print('[INFO] - Retrieving tweets between ' + str(start_date) + ' and ' + str(end_date))
    
        # get json response to list
        tweets = list(rs.stream())
        
        # break free from while loop
        break
    except Exception as err:
        if tries == 0:
            raise err
        else:
            print('[INFO] - Got connection error, waiting 15 seconds and trying again. ' + str(tries) + ' tries left.')
            time.sleep(15)

# parse results to dataframe
print('[INFO] - Parsing collected tweets from ' + str(start_date) + ' to ' + str(end_date))
tweetdf = v2parser(tweets, search_config['results_per_call'])

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
file_prefix_w_date = search_config['filename_prefix'] + start_date.isoformat()
outpickle = file_prefix_w_date + '.pkl'
outcsv = file_prefix_w_date + '.csv'

# save to file
if args['output'] == 'pkl':
    # save to pickle
    tweetdf.to_pickle(outpickle)
elif args['output'] == 'csv':
    # save to csv
    tweetdf.to_csv(outcsv, sep=';', encoding='utf-8')

print('[INFO] - ... done!')