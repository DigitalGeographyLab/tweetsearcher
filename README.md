# tweetsearcher
A Python tool to download Tweets using Academic Research credentials and the API v2.

## What does it do?
tweetsearcher is a Python tool designed to download Tweets from Twitter using Academic Research credentials. It downloads and automatically parses the json response from Twitter's API v2 saving it to a pickled dataframe. If one is inclined to do so, there's another script file included which will turn the pickled dataframes into geopackage files, commonly used in GIS.

## Why does it exist?
This work is based on Seija Sirkiä's work in creating a collector tool that worked with Twitter's Premium API, but has been considerably rewritten to work with the current version of Twitter's searchtweets-v2 python library. It was made partly to promote open tools and open science, but also as there seemed to be a need for a ready-made and (somewhat) easy-to-use tool to collect Twitter data for academic research.

## Usage
You need to have Python 3.8 or newer installed, preferrably the `miniconda` distribution if you want to use the environment `.yml` file.

Clone this repository with `git clone https://github.com/DigitalGeographyLab/tweetsearcher.git`. When that's ready, we recommend you create a virtual environment and install the requirements file with `conda env create -f tweetsearcher_env.yml`.

Set up the credentials file `.search_creds.yaml` by replacing the consumer key, secret and bearer token with your own:

```
search_tweets_v2:
  endpoint:  https://api.twitter.com/2/tweets/search/all
  consumer_key: <CONSUMER_KEY>
  consumer_secret: <CONSUMER_SECRET>
  bearer_token: <BEARER_TOKEN>
```

and the search parameters file `search_config.yaml` , replace the values with ones you want:

```
search_rules:
    start_time: 2020-01-01
    end_time: 2020-05-01
    query: (snow OR rain) place_country:FI -is:retweet

search_params:
    results_per_call: 500
    max_tweets: 1000000

output_params:
    save_file: True
    filename_prefix: my_weather_search
    results_per_file: 1000000
```

For example: The above search parameters file would search for tweets mentioning snow or rain that have been geotagged in Finland between January 1st and May 1st in 2020 and which are NOT retweets. *Please note that the end time date **IS NOT** collected, the collection stops at 23:59:59 the previous date, in this case on the 30th of April*. The parameters would return maximum of 500 results per call and a maximum of 100 000 tweets. The file would be saved with the prefix `my_weather_search` and one file would contain a maximum of 1 000 000 tweets. For more specific instructions on how to build a search query see Twitter's documentation: https://developer.twitter.com/en/docs/twitter-api/tweets/search/integrate/build-a-query

Then just navigate to the repository directory on your local machine and type `python tweets_to_file.py -f foo -b bar -test` and you after a while you should start accumulating pickled dataframes (`.pkl` files). They can be read into python with `pandas` library.

If you want to convert the files into a GeoPackage file (a common spatial file format) then run `python tweets_to_gpkg.py` in the directory where you have the `.pkl` files stored in. Then you can open them in QGIS and other GIS software like ArcGIS.

## Referencing

If you use this script in your research or project, please refer back to us using this:

Väisänen, T., S. Sirkiä (2021) tweetsearcher: A python tool for downloading Tweets for academic research. DOI: nothing/yet/here

Also in BibTeX:
```
Here be BibTeX and Zenodo DOI
```

## Other
If this tool isn't quite what you're looking for, consider taking a look at this nice tool for collecting Twitter data using Academic Research credentials is this one by Christoph Fink: https://gitlab.com/christoph.fink/twitterhistory
