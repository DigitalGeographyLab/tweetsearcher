#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 19 11:29:53 2021

@author: waeiski
"""
# import glob
import pandas as pd
import geopandas as gpd
import glob
import argparse

# Set up the argument parser
ap = argparse.ArgumentParser()

# Get starting date
ap.add_argument("-o", "--output", required=True,
                help="Name of the output geopackage file. For example: "
                " my_tweets.gpkg ")

# Parse arguments
args = vars(ap.parse_args())

# create empty list for file paths
filelist = []
# populate list with file paths
for pickle in glob.glob('*.pkl'):
    filelist.append(pickle)

# create empty list for dataframes
dflist = []
# popualte list with dataframes
print('[INFO] - Reading pickled dataframes...')
for file in filelist:
    data = pd.read_pickle(file)
    dflist.append(data)

# concatenate dataframes into one dataframe
data = pd.concat(dflist, ignore_index=True)

# loop over data
print('[INFO] - Parsing coordinate information...')
for i, row in data.iterrows():
    # parse gps coordinates if present
    if row['geo.coordinates.x'] != None:
        data.at[i, 'locinfo_type'] = 'gps'
        data.at[i, 'x_coord'] = row['geo.coordinates.x']
        data.at[i, 'y_coord'] = row['geo.coordinates.y']
    # parse bounding box coordinates if no gps coordinates
    elif row['geo.coordinates.x'] == None:
        data.at[i, 'locinfo_type'] = 'bbox'
        data.at[i, 'x_coord'] = row['geo.centroid.x']
        data.at[i, 'y_coord'] = row['geo.centroid.y']

# convert NaN to None
data = data.where(pd.notnull(data), None)

# drop rows without any coordinates
data = data.dropna(subset=['x_coord', 'y_coord']).reset_index()
    
# drop columns not needed
data = data[['id', 'author_id', 'created_at', 'reply_settings', 'conversation_id',
       'source', 'in_reply_to_user_id', 'text', 'possibly_sensitive', 'lang',
       'referenced_tweets.id', 'referenced_tweets.author_id', 'referenced_tweets.type',
       'public_metrics.retweet_count', 'public_metrics.reply_count',
       'public_metrics.like_count', 'public_metrics.quote_count',
       'user.description', 'user.verified',
       'user.id', 'user.protected', 'user.url',
       'user.location', 'user.name', 'user.created_at', 'user.username',
       'user.public_metrics.followers_count',
       'user.public_metrics.following_count',
       'user.public_metrics.tweet_count',
       'geo.place_id', 'geo.coordinates.type', 'geo.coordinates.x',
       'geo.coordinates.y', 'geo.full_name', 'geo.name',
       'geo.place_type', 'geo.country', 'geo.country_code', 'geo.type',
       'locinfo_type', 'x_coord', 'y_coord']]

# generate geometry
print('[INFO] - Generating geometry information from coordinates...')
gdf = gpd.GeoDataFrame(data, geometry=gpd.points_from_xy(data.x_coord, data.y_coord))

# set crs to WGS-84
gdf = gdf.set_crs("EPSG:4326")

# save to geopackage
print('[INFO] - Saving to geopackage...')
gdf.to_file(args['output'], driver='GPKG')

print('[INFO] - ... done!')