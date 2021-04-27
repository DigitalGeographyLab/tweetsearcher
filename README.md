# tweetsearcher
A Python tool to download Tweets using Academic Research credentials and the API v2 developed at [Digital Geography Lab](https://www2.helsinki.fi/en/researchgroups/digital-geography-lab).

## What does it do?
tweetsearcher is a Python tool designed to download Tweets from Twitter using Academic Research credentials. It downloads and automatically parses the json response from Twitter's API v2 saving it to a pickled dataframe. If one is inclined to do so, there's another script file included which will turn the pickled dataframes into geopackage files, commonly used in GIS.

## Why does it exist?
This work is based on Seija Sirkiä's work in creating a collector tool that worked with Twitter's Premium API, but has been considerably rewritten to work with the current version of Twitter's searchtweets-v2 python library. It was made partly to promote open tools and open science, but there also seemed to be a need for a ready-made and (somewhat) easy-to-use tool to collect Twitter data for academic research.

## Set up
You need to have Python 3.8 or newer installed, preferrably the [miniconda distribution of Python](https://docs.conda.io/en/latest/miniconda.html) if you want to use the environment `.yml` file.

Clone this repository with `git clone https://github.com/DigitalGeographyLab/tweetsearcher.git` or download the zip file. When that's ready, we recommend you create a virtual environment and install the requirements file with `conda env create -f tweetsearcher_env.yml`.

Create a credentials file `.twitter_keys.yaml` with a text editor of your choice, copy-paste the template below and replace the consumer key, secret and bearer token with your own:

```
search_tweets_v2:
  endpoint:  https://api.twitter.com/2/tweets/search/all
  consumer_key: <CONSUMER_KEY>
  consumer_secret: <CONSUMER_SECRET>
  bearer_token: <BEARER_TOKEN>
```

and create the search parameters file `search_config.yaml` with a text editor of your choice and replace the values with ones you want. For more specific instructions on how to build a search query see Twitter's documentation: https://developer.twitter.com/en/docs/twitter-api/tweets/search/integrate/build-a-query

```
search_rules:
    query: (snow OR rain) place_country:FI -is:retweet
    tag: my_weather_test

search_params:
    results_per_call: 500
    max_tweets: 1000000

output_params:
    filename_prefix: my_weather_search
    results_per_file: 1000000
```

For example: The above search config file would search for tweets mentioning snow or rain that have been geotagged in Finland and which are NOT retweets. The time window from which these tweets are searched from is defined when giving the command (see below). The parameters would return maximum of 500 results per call and a maximum of 100 000 tweets. The resulting file would be saved with the prefix `my_weather_search` and one file would contain a maximum of 1 000 000 tweets. If you want to set up a daily collection, remove `start_time` and `end_time` from the config, then the script will collect tweets from yesterday (i.e. the day before the current day). 

## Usage

Then just navigate to the cloned repository directory on your local machine and type:
```
python v2_tweets_to_file.py -sd 2020-01-28 -ed 2020-05-29 -o pickle -w 45
```
and you after a while you should start accumulating pickled dataframes (`.pkl` files) one per date, so if you're requesting a full year then you'll be getting 365 files. They can be read into python with `pandas` library for easier manipulation. `csv` files are also supported, but some fields containing data types like `list` and `dict` objects will be converted to plaintext. The flags stand for `sd` = start date, `ed` = end date, `o` = output file format and `w` = wait time in seconds. Wait time is there to be used if you think you're going to hit the Twitter rate limits, for example when downloading a large dataset like a full year of geotagged tweets from Finland. *Please note that the end time date **IS NOT** collected, the collection stops at 23:59:59 the previous date, in the example case on the 28th of May at 23:59:59*.

You can then combine all the daily `pkl` files with `combine_tweets.py` file. It supports saving to a [GeoPackage](https://www.geopackage.org/) file (a common spatial file format like shapefile), a pickled Pandas dataframe and a plain csv file. To combine tweets run the following command in the directory where you have the `.pkl` files:
```
python tweets_to_gpkg.py -f gpkg -o my_tweets.gpkg
```
The script a geopackage file called `my_tweets.gpkg` in the WGS-84 crs, which you can open in QGIS and other GIS software like ArcGIS. Combining tweets from `csv` files hasn't been implemented.

## Notes on the output

The script does some reshuffling and renaming of the "raw" output json, mostly out of necessity (duplicate field names etc.) but partly for convenience (similar fields are next to each other). The output file will have individual tweets connected with the requested expansions (like place, media etc) unlike with the raw output where they're as a separate json object. However, for referenced tweets it only returns the referenced tweet id and author id. If there are geotags, the output file will signify whether they're based on gps coordinates or a bounding box centroids, if both are present the gps coordinates are preferred. Please note that the timestamp in the `created_at` field is a UTC timestamp and you may want to convert it to a local time zone if you're doing temporal analysis.

The geopackage export script will drop some columns containing unparsed `dict` and `list` data types, because they're not supported by the file format.

The csv files use semicolon (;) as the separator and utf-8 as their encoding.

If you're not interested in what bots have to say, then you have to do the cleaning up yourself. Checking the `source` of the tweet and removing all posts from sources that seem bot-like or automated is a simple first step. There are plenty of bots posting weather data, satellite positions, every dicionary word in a given language etc. After initial bot cleaning, you can use [Botometer](https://botometer.osome.iu.edu/) to do account-specific checking for the rest (the free option has a 500 account daily quota).

## Referencing

If you use this script in your research or project, please refer back to us using this:

Väisänen, T., S. Sirkiä, O. Järv & T. Toivonen (2021) tweetsearcher: A python tool for downloading Tweets for academic research. DOI: nothing/yet/here

Also in BibTeX:
```
Here be BibTeX and Zenodo DOI
```

## Other options
If this tool isn't quite what you're looking for, consider taking a look at this nice tool for collecting Twitter data using Academic Research credentials is this one by Christoph Fink: https://gitlab.com/christoph.fink/twitterhistory
