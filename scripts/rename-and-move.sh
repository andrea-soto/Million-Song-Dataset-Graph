#!/usr/bin/env bash

folders=(nodes_artists nodes_songs nodes_albums nodes_years nodes_tags rel_similar_artists rel_performs rel_artist_has_album rel_artist_has_tag rel_song_in_album rel_similar_songs rel_song_has_tag rel_song_year)
newName=(artist song album year tag sim-a perf has-album a-has-tag in-album sim-s s-has-tag release)

# Rename files
echo LOG: Renaming files...
for i in {0..12}
do 
rename part-00 ${newName[$i]}- /graph/import/tmp/${folders[$i]}/part*
done

echo LOG: Copying files to /graph/neo4j/data/import...
for i in {0..12}
do 
cp /graph/import/tmp/${folders[$i]}/${newName[$i]}* /graph/neo4j/data/import/
done

echo LOG: Creating headers...
# Create header CSV files
echo "id:ID(artist),idmb,id7d,name" > /graph/neo4j/data/import/hdr-artist.csv
echo "songid,trackid:ID(song),title,danceability:FLOAT,duration:FLOAT,energy:FLOAT,loudness:FLOAT" > /graph/neo4j/data/import/hdr-song.csv
echo "name:ID(album)" > /graph/neo4j/data/import/hdr-album.csv
echo "year:ID(year)" > /graph/neo4j/data/import/hdr-year.csv
echo "tag:ID(tag)" > /graph/neo4j/data/import/hdr-tag.csv
 
echo ":START_ID(artist),:END_ID(artist)" > /graph/neo4j/data/import/hdr-sim-a.csv
echo ":START_ID(artist),:END_ID(song)" > /graph/neo4j/data/import/hdr-perf.csv
echo ":START_ID(artist),:END_ID(album)" > /graph/neo4j/data/import/hdr-has-album.csv
echo ":START_ID(artist),:END_ID(tag),frq:FLOAT,weight:FLOAT" > /graph/neo4j/data/import/hdr-a-has-tag.csv
echo ":START_ID(song),:END_ID(album)" > /graph/neo4j/data/import/hdr-in-album.csv
echo ":START_ID(song),:END_ID(song),weight:FLOAT" > /graph/neo4j/data/import/hdr-sim-s.csv
echo ":START_ID(song),:END_ID(tag),weight:FLOAT" > /graph/neo4j/data/import/hdr-s-has-tag.csv
echo ":START_ID(song),:END_ID(year)" > /graph/neo4j/data/import/hdr-release.csv