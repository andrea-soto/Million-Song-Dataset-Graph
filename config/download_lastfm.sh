#!/usr/bin/env bash

mkdir /graph/import
mkdir /graph/lastfm

cd /graph/lastfm
mkdir data

wget http://labrosa.ee.columbia.edu/millionsong/sites/default/files/lastfm/lastfm_train.zip
wget http://labrosa.ee.columbia.edu/millionsong/sites/default/files/lastfm/lastfm_test.zip

unzip -q lastfm_train.zip
mv lastfm_train/* data/

unzip -q lastfm_test.zip
rsync -av lastfm_test/ data/

rm lastfm_train.zip
rm lastfm_test.zip
rm -r lastfm_train/
rm -r lastfm_test/

cd /graph/import
wget http://labrosa.ee.columbia.edu/millionsong/sites/default/files/tasteprofile/sid_mismatches.txt
    
cd /graph/