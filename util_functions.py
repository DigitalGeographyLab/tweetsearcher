#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May 17 09:43:17 2021

INFO
####

This file contains untility functions for tweetsearcher A Python tool for
downloading tweets using for academic research. The script works with Twitter's
Academic Research product track credentials and the Twitter API v2.


USAGE
#####

Add new functions here, otherwise leave this file be.


NOTE
####

Some of these functions are clunky and represent a minimum viable product. Further
development is encouraged, but please be careful of not breaking any
functionality. Create a new branch, test, test, test, and send a pull request.

@author: Tuomas Väisänen
"""

from shapely.geometry import Point, Polygon
import pandas as pd
import copy
from datetime import timedelta

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
        for i, item in refs.items():
            
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
    for i, item in medf.items():
        
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
    
    # check if coordinates exist
    if 'geo.coordinates.coordinates' in outdf.columns.tolist():
        
        # loop over df
        for i, row in outdf.iterrows():
                
            # check if row contains coordinate list
            if type(row['geo.coordinates.coordinates']) == list:
                
                # extract x and y coords
                outdf.at[i, 'geo.coordinates.x'] = row['geo.coordinates.coordinates'][0]
                outdf.at[i, 'geo.coordinates.y'] = row['geo.coordinates.coordinates'][1]
                
    else:
        outdf['geo.coordinates.coordinates'] = None
        outdf['geo.coordinates.x'] = None
        outdf['geo.coordinates.y'] = None
        
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
    
    # try concatenating referenced tweets expansion data
    try:
        refdf = pd.concat(reflist, ignore_index=True)
        refdf = refdf.drop_duplicates()
    except:
        print('[INFO] - No referenced tweets at all in response.')
    
    # concatenate user expansion data
    userdf = pd.concat(userlist, ignore_index=True)
    userdf = userdf.drop_duplicates(subset=['user.id'])
    
    # try concatenating place expansion
    try:
        placedf = pd.concat(placelist, ignore_index=True)
        placedf = placedf.drop_duplicates(subset=['geo.id'])
    except:
        print('[INFO] - No place expansions at all in response.')
    
    # try concatenating media expansion
    try:
        mediadf = pd.concat(medialist, ignore_index=True)
        
        # parse media types
        twtdf = media_parse(twtdf, mediadf)
    except:
        print('[INFO] - No media expansions at all in response.')
    
    # combine together
    print('[INFO] - Combining tweets and expansions to one dataframe...')
    outdf = pd.merge(twtdf, userdf, left_on='author_id', right_on='user.id')
    
    # connect places to tweets if places are present
    try:
        outdf = pd.merge(outdf, placedf, left_on='geo.place_id', right_on='geo.id', how='left')
    except:
        pass
    
    # connected referenced tweets to original tweets if referenced tweets are present
    try:
        outdf = pd.merge(outdf, refdf, left_on='referenced_tweets.id', right_on='referenced_tweets.tweet_id',
                         how='left')
    except:
        pass
    
    
    # convert NaNs to Nones
    outdf = outdf.where(pd.notnull(outdf), None)
    
    # drop irrelevant columns from last round
    try:
        outdf = outdf.drop(columns=['attachments.poll_ids'])
    except:
        print('[INFO] - No poll ids found')
        
    # print full length of dataframe
    print('[INFO] - Dataframe size ' + str(len(outdf)) + ' tweets.')

    # give the output back
    return outdf