# tweetsearcher
A Python tool to download Tweets using Academic Research credentials and the API v2.

## What does it do?
tweetsearcher is a Python tool designed to download Tweets from Twitter using Academic Research credentials. It downloads and automatically parses the json response from Twitter's API v2 saving it to a pickled dataframe. If one is inclined to do so, there's another script file included which will turn the pickled dataframes into geopackage files, commonly used in GIS.

## Why does it exist?
This work is based on Seija Sirkiä's work in creating a collector tool that worked with Twitter's Premium API, but has been considerably rewritten to work with the current version of Twitter's searchtweets-v2 python library. It was made partly to promote open tools and open science, but also as there seemed to be a need for a ready-made and (somewhat) easy-to-use tool to collect Twitter data for academic research.

## Usage
You need to have Python 3.8 or newer installed. When that's ready, we recommend you create a virtual environment and install the requirements.txt file with `conda install requirements.txt`. Finally clone this repository with `git clone https://github.com/DigitalGeographyLab/tweetsearcher.git`.

Then just navigate to the repository directory on your local machine and type `python tweets_to_file.py -f foo -b bar -test` and you after a while you should start accumulating pickled dataframes (`.pkl` files). 

If you want to convert the files into a GeoPackage file (a common spatial file format) then run `python tweets_to_gpkg.py` in the directory where you have the `.pkl` files stored in.

## Referencing

If you use this script in your research or project, please refer back to us using this:

```
Here be BibTeX and Zenodo DOI
```

## Other
If this tool isn't quite what you're looking for, consider taking a look at this nice tool for collecting Twitter data using Academic Research credentials is this one by Christoph Fink: https://gitlab.com/christoph.fink/twitterhistory
