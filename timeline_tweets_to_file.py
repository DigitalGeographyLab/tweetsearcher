#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr  9 11:04:24 2021

INFO
####

This script downloads Tweets from the full archive of Twitter using the academic
access API. It downloads tweets one day at a time to reduce chance of hitting
rate limits from the API and saves them as a pickled dataframe (.pkl).

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

@author: Tuomas Väisänen & Seija Sirkiä
"""

from util_functions import v2parser, daterange
from searchtweets import ResultStream, gen_request_parameters, load_credentials, read_config
from datetime import datetime, timedelta
import time
import argparse
import tweepy
import pandas as pd

# Set up the argument parser
ap = argparse.ArgumentParser()

# Get userlist csv
ap.add_argument("-ul", "--userlist", required=True,
                help="Path to userlist CSV file")

# get start date
ap.add_argument("-sd", "--startdate", required=True,
                type=lambda s: datetime.strptime(s, '%Y-%m-%d'),
                help="Start date of the collection in the following form: "
                " YEAR-MO-DA for example 2018-01-01")

# Get end date
ap.add_argument("-ed", "--enddate", required=True,
                type=lambda s: datetime.strptime(s, '%Y-%m-%d'),
                help="End date of the collection in the following form: "
                " YEAR-MO-DA for example 2018-02-18")

# get save format
ap.add_argument("-o", "--output", required=True, default='pkl',
                help="Output file format, valid options are either pkl or csv. "
                "Default: pkl")

# get wait time
ap.add_argument("-w", "--wait", required=False, default=45,
                help="Set wait time between requests to avoid Twitter rate limits. "
                "Default: 45")

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

# read user list here
users = pd.read_csv(args['userlist'])

# convert to list
users = users['usr_id'].values.tolist()

# load twitter keys
search_creds = load_credentials('.twitter_keys.yaml',
                               yaml_key = 'search_tweets_v2',
                               env_overwrite = False)

# load configuration for search query
config = read_config('search_config.yaml')

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

for user in users:
    
    # form search query per user and rule out retweets, replies and quote tweets
    search_q = 'from:{} -is:retweet -is:reply -is:quote'.format(user)
    search_q = 'from:{}'.format(user)
    # payload rules for v2 api
    rule = gen_request_parameters(query = search_q,
                                  results_per_call = config['results_per_call'],
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
                      max_tweets = config['max_tweets'],
                      **search_creds)
    
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
            
            # wait for 7 seconds to not hit rate limits if zero tweets
            time.sleep(7)
            
            # break free from while loop
            break
        except Exception as err:
            if tries == 0:
                raise err
            else:
                print('[INFO] - Got connection error, waiting 15 seconds and trying again. ' + str(tries) + ' tries left.')
                time.sleep(15)
    
    # parse results to dataframe
    print('[INFO] - Parsing collected tweets of user ' + str(user) + 'from ' + str(start_date) + ' to ' + str(end_date))
    tweetdf = v2parser(tweets, config['results_per_call'])
    
    # try to order columns semantically
    try:
        tweetdf = tweetdf[['id', 'author_id', 'created_at', 'reply_settings', 'conversation_id',
                           'in_reply_to_user_id', 'text', 'possibly_sensitive',
                           'lang', 'referenced_tweets', 'referenced_tweets.id', 
                           'referenced_tweets.author_id', 'referenced_tweets.type',
                           'public_metrics.retweet_count', 'public_metrics.reply_count',
                           'public_metrics.like_count', 'public_metrics.quote_count',
                           'entities.mentions', 'entities.urls', 'entities.hashtags',
                           'entities.annotations', 'attachments.media_keys',
                           'attachments.media_types', 'user.description', 'user.verified', 'user.id', 'user.protected',
                           'user.url', 'user.profile_image_url', 'user.location', 'user.name',
                           'user.created_at', 'user.username', 'user.public_metrics.followers_count',
                           'user.public_metrics.following_count', 'user.public_metrics.tweet_count',
                           'user.public_metrics.listed_count', 'user.entities.description.hashtags',
                           'user.entities.url.urls', 'user.entities.description.mentions',
                           'user.entities.description.urls', 'geo.place_id', 'geo.coordinates.type',
                           'geo.coordinates.coordinates', 'geo.coordinates.x', 'geo.coordinates.y',
                           'geo.full_name', 'geo.name', 'geo.place_type', 'geo.country',
                           'geo.country_code', 'geo.type', 'geo.bbox', 'geo.centroid',
                           'geo.centroid.x', 'geo.centroid.y']]
    except:
        pass
    
    # set up file prefix from config
    file_prefix_w_date = config['filename_prefix'] + start_date.isoformat()
    outpickle = file_prefix_w_date + '_user_' + str(user) + '.pkl'
    outcsv = file_prefix_w_date + '.csv'
    
    # save to file
    if args['output'] == 'pkl':
        # save to pickle
        tweetdf.to_pickle(outpickle)
    elif args['output'] == 'csv':
        # save to csv
        tweetdf.to_csv(outcsv, sep=';', encoding='utf-8')

print('[INFO] - ... done!')