#!/usr/bin/env bash

# Download data subset of 10,000 songs, ~1GB to develop and test code
wget http://static.echonest.com/millionsongsubset_full.tar.gz
wait
tar xvzf millionsongsubset_full.tar.gz
wait
rm millionsongsubset_full.tar.gz

cd MillionSongSubset

# Download last-fm data with song similarities
wget http://labrosa.ee.columbia.edu/millionsong/sites/default/files/lastfm/lastfm_subset.zip
unzip lastfm_subset.zip
rm lastfm_subset.zip

cd ..