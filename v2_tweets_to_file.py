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
from searchtweets import ResultStream, gen_request_parameters, load_credentials, read_config
from datetime import timedelta, datetime
from shapely.geometry import Point, Polygon
import pandas as pd
import copy
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
ap.add_argument("-o", "--output", required=True, default='pickle',
                help="Output file format, valid options are either pickle or csv. "
                "Default: pickle")

# get wait time
ap.add_argument("-w", "--wait", required=True, default=45,
                help="Set wait time between requests to avoid Twitter rate limits. "
                "Default: 45")

# Parse arguments
args = vars(ap.parse_args())

# get waittime
waittime = int(args['wait'])

# define date range function
def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

# function to parse references in original tweets
def ref_parse(tweets):
    
    # get initial output df
    outdf = tweets.copy()
    
    # create empty column
    outdf['referenced_tweets.id'] = None
    outdf['referenced_tweets.type'] = None
    
    # get inital result of unlisted ref dicts
    try:
        
        refs = tweets['referenced_tweets']
        
        # loop over references
        for i, item in refs.iteritems():
            
            # check if there are references on row
            if type(item) == list:
                
                # create empty lists
                refids = []
                reftypes = []
                
                # loop over refs and append to list
                for ref in item:
                    reftypes.append(ref['type'])
                    refids.append(ref['id'])
                    
                # convert refs to string
                refids = ';'.join(refids)
                reftypes = ';'.join(reftypes)
                
                # add parsed refs to correct cell
                outdf.at[i, 'referenced_tweets.id'] = refids
                outdf.at[i, 'referenced_tweets.type'] = reftypes
    except:
        pass
        
    return outdf

# function to parse entity annotations
def media_parse(tweets, media):
    # get initial output df
    outdf = tweets.copy()
    
    # create column
    outdf['attachments.media_types'] = None
    
    # get inital results
    medf = tweets['attachments.media_keys']
    
    # loop over media keys
    for i, item in medf.iteritems():
        
        # placholder for media types
        types = None
        
        # check if current row has media key
        if type(item) == list:
            
            # empty list for media types
            types = []
            
            # loop over keys per tweet
            for key in item:
                med = media[media['media_key'] == key]
                types.append(med['type'])
                
        # add types to correct index and column
        outdf.at[i, 'attachments.media_types'] = types
    
    # give output
    return outdf

# funcion to parse coordinates
def coord_parse(tweets):
    # get initial copy of df
    outdf = tweets.copy()
    
    # loop over df
    for i, row in outdf.iterrows():
        
        # check if row contains coordinate list
        if type(row['geo.coordinates.coordinates']) == list:
            
            # extract x and y coords
            outdf.at[i, 'geo.coordinates.x'] = row['geo.coordinates.coordinates'][0]
            outdf.at[i, 'geo.coordinates.y'] = row['geo.coordinates.coordinates'][1]
    
    # return output df
    return outdf

# function to calculate bbox centroid
def bbox_centroid(coords):
    '''
    Assumes coordinates are in a list in following order:
        [east, south, west, north]
    '''
    # get cardinalities
    se = Point(coords[0], coords[1])
    ne = Point(coords[0], coords[3])
    nw = Point(coords[2], coords[3])
    sw = Point(coords[2], coords[1])
    
    # get polygon
    bbox = Polygon([se, ne, nw, sw])
    
    # get centroid x and y
    cx = bbox.centroid.x
    cy = bbox.centroid.y
    
    # return coordinate pair
    cpair = [cx, cy]
    return cpair

# function to parse and combine v2 responses
def v2parser(tweets, maxcalls):
    
    # get unmutable copy of tweets
    tweetlist = copy.deepcopy(tweets)
        
    # placeholder list for dataframes
    twtlist = []
    reflist = []
    placelist = []
    userlist = []
    medialist = []
    
    # get locations of all ends of calls 
    end_idx = [i for i, d in enumerate(tweetlist) if "result_count" in d.keys()]
    
    # get indicator numbers for print messages
    rounds = len(end_idx)
    cur_round = 1
    
    # loop over end indices
    for i, pos in enumerate(end_idx):
        
        # print indicator numbers
        print('[INFO] - Processing round ' + str(cur_round) + ' from ' + str(rounds))
        
        # get accurate slicing off points
        actual_pos = pos + 1
        
        # check if first call
        if i == 0:
            
            # get first slice
            tweetslice = tweetlist[:actual_pos]
            
        else:
            
            # get position where previous call ended
            prev_pos = end_idx[i - 1] + 1
            
            # slice current tweets
            tweetslice = tweetlist[prev_pos:actual_pos]

        # get tweets
        twts = tweetslice[:-2] # get all tweets per request max
        
        # dataframefy
        twts = pd.json_normalize(twts)
        
        # parse point coordinates
        twts = coord_parse(twts)
        
        # parse refs from original tweets
        twts = ref_parse(twts)
        
        # append to tweetlist
        twtlist.append(twts)
        
        # try getting referenced tweets expansion
        try:
            # get referenced tweets expansion
            rftwts = tweetslice[-2]['tweets']
            
            # dataframefy
            rftwts = pd.json_normalize(rftwts)
            
            # drop unnecessary columns (ask Olle) from referenced tweets
            rftwts = rftwts[['id','author_id']].rename(columns={'author_id':'referenced_tweets.author_id',
                                                                'id':'referenced_tweets.tweet_id'})
            # append to reflist
            reflist.append(rftwts)
            
        except:
            print('[INFO] - No "tweets" expansion found in round: ' + str(cur_round))
        
        # try getting places expansion
        try:
            # get places expansion
            places = tweetslice[-2]['places']
            
            # dataframefy
            places = pd.json_normalize(places)
            
            # rename places columns
            places = places.rename(columns={'country_code':'geo.country_code',
                                            'place_type':'geo.place_type',
                                            'full_name':'geo.full_name',
                                            'id':'geo.id',
                                            'country':'geo.country',
                                            'name':'geo.name'})
            
            # calculate bbox centroid
            places['geo.centroid'] = places['geo.bbox'].apply(bbox_centroid)
            
            # get centroid x and y coordinates
            places['geo.centroid.x'] = places['geo.centroid'].apply(lambda x: x[0])
            places['geo.centroid.y'] = places['geo.centroid'].apply(lambda x: x[1])
            
            # append to placelist
            placelist.append(places)
            
        except:
            print('[INFO] - No "places" expansion found in round: ' + str(cur_round))
        
        # try getting user expansion
        try:
            # get user expansion
            users = tweetslice[-2]['users']
            
            # dataframefy
            users = pd.json_normalize(users)
            
            # make user data joinable
            users = users.add_prefix('user.')
            
            # append to userlist
            userlist.append(users)
            
        except:
            print('[INFO] - No "users" expansion found in round: ' + str(cur_round))
        
        # try geting media expansion
        try:
            # get media expansion
            media = tweetslice[-2]['media']
            
            # dataframefy
            media = pd.json_normalize(media)
            
            # append to medialist
            medialist.append(media)
            
        except:
            print('[INFO] - No "media" expansion found in round: ' + str(cur_round))        
        
        # update current round indicator
        cur_round += 1
        
    # combine dataframes collected in rounds
    print('[INFO] - Combining tweet and expansion dataframes..')
    twtdf = pd.concat(twtlist, ignore_index=True)
    
    refdf = pd.concat(reflist, ignore_index=True)
    refdf = refdf.drop_duplicates()
    
    userdf = pd.concat(userlist, ignore_index=True)
    userdf = userdf.drop_duplicates(subset=['user.id'])
    
    placedf = pd.concat(placelist, ignore_index=True)
    placedf = placedf.drop_duplicates(subset=['geo.id'])
    
    mediadf = pd.concat(medialist, ignore_index=True)
    
    # parse media types
    twtdf = media_parse(twtdf, mediadf)
    
    # combine together
    print('[INFO] - Combining tweets and expansions to one dataframe...')
    outdf = pd.merge(twtdf, placedf, left_on='geo.place_id', right_on='geo.id', how='left')
    outdf = pd.merge(outdf, userdf, left_on='author_id', right_on='user.id')
    outdf = pd.merge(outdf, refdf, left_on='referenced_tweets.id', right_on='referenced_tweets.tweet_id',
                     how='left')
    
    
    # convert NaNs to Nones
    outdf = outdf.where(pd.notnull(outdf), None)
    
    # drop irrelevant columns from last round
    try:
        outdf = outdf.drop(columns=['attachments.poll_ids'])
    except:
        print('[INFO] - No poll ids found')

    # give the output back
    return outdf

# check if output filetypes are valid
if args['output'] == 'pickle':
    # save to pickle
    print('[INFO] - Output file set to pickle')
elif args['output'] == 'csv':
    # save to csv
    print('[INFO] - Output file set to csv')
else:
    print('[INFO] - Invalid output file! Valid options are pickle or csv. Exiting...')
    exit

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
                        "reply_settings", "source", "text", "withheld",])
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

# loop through dates
for single_date in daterange(start_date, end_date):
    
    # set start timestamp
    start_ts = single_date
    
    # set end timestamp
    end_ts =  single_date + timedelta(days=1)
    
    # payload rules for v2 api
    rule = gen_request_parameters(query = config['query'],
                            results_per_call = config['results_per_call'],
                            start_time = start_ts.isoformat(),
                            end_time = end_ts.isoformat(),
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
    
    # indicate which day is getting retrieved
    print('[INFO] - Retrieving tweets from ' + str(start_ts))

    # get json response to list
    tweets = list(rs.stream())
    
    # parse results to dataframe
    print('[INFO] - Parsing tweets from ' + str(start_ts))
    tweetdf = v2parser(tweets, 500)
    
    # order columns semantically
    tweetdf = tweetdf[['id', 'author_id', 'created_at', 'reply_settings', 'conversation_id',
                       'source', 'in_reply_to_user_id', 'text', 'possibly_sensitive',
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
    
    # set up file prefix from config
    file_prefix_w_date = config['filename_prefix'] + start_ts.isoformat()
    outpickle = file_prefix_w_date + '.pkl'
    outcsv = file_prefix_w_date + '.csv'
    
    # save to file
    if args['output'] == 'pickle':
        # save to pickle
        tweetdf.to_pickle(outpickle)
    elif args['output'] == 'csv':
        # save to csv
        tweetdf.to_csv(outcsv, sep=';', encoding='utf-8')
    
    # sleeps to not hit request limit so soon
    time.sleep(waittime) 

print('[INFO] - ... done!')