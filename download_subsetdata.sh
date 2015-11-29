#!/usr/bin/env bash

#Create a directory for the data
mkdir data
cd data

# Download data subset of 10,000 songs, ~1GB to develop and test code
wget http://static.echonest.com/millionsongsubset_full.tar.gz data_subset
wait

tar xvzf millionsongsubset_full.tar.gz
wait

# Download list of all artist ID 
# The format is: artist id<SEP>artist mbid<SEP>track id<SEP>artist name
wget http://labrosa.ee.columbia.edu/millionsong/sites/default/files/AdditionalFiles/unique_artists.txt
wait
wc -l unique_artists.txt #44745 unique_artists.txt

# Download list of all unique artist terms (Echo Nest tags) 
wget http://labrosa.ee.columbia.edu/millionsong/sites/default/files/AdditionalFiles/unique_terms.txt
wait
wc -l unique_terms.txt #7643 unique_terms.txt
    
# Download list of all unique artist musicbrainz tags
wget http://labrosa.ee.columbia.edu/millionsong/sites/default/files/AdditionalFiles/unique_mbtags.txt
wait
wc -l unique_mbtags.txt #2321 unique_mbtags.txt

cd ..