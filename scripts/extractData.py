#!/usr/bin/env python
import os
import glob
import sys
import shutil
from pyspark import SparkContext
import numpy as np
import h5py
import json

def parse_mismatches(line):
    '''
    This function extracts the songID and trackID of the mismatched records.
    Returned value: ('songID', 'trackID')
    '''
    return line[8:45].split()


def get_h5_info(path):
    '''
    Takes a path to a song stored as an HDF5 file and returns a dictionary with the 
    information that will be included in the graph
    ''' 
    d = {}
    with h5py.File(path, 'r') as f:
        song_id = f['metadata']['songs']['song_id'][0]
        track_id = f['analysis']['songs']['track_id'][0]
        
        if [song_id, track_id] not in songsToRemove.value:

            # --- Artist Info -----------------------------
            d.setdefault('artist_id', f['metadata']['songs']['artist_id'][0])
            d.setdefault('artist_mbid', f['metadata']['songs']['artist_mbid'][0])
            d.setdefault('artist_7did', f['metadata']['songs']['artist_7digitalid'][0])
            d.setdefault('artist_name', f['metadata']['songs']['artist_name'][0])

            # --- Song Info -----------------------------
            d.setdefault('song_id', song_id)
            d.setdefault('track_id', track_id)
            d.setdefault('title', f['metadata']['songs']['title'][0])
            d.setdefault('dance', f['analysis']['songs']['danceability'][0])
            d.setdefault('dur', f['analysis']['songs']['duration'][0])
            d.setdefault('energy', f['analysis']['songs']['energy'][0])
            d.setdefault('loudness', f['analysis']['songs']['loudness'][0])

            # --- Year -----------------------------
            d.setdefault('year', f['musicbrainz']['songs']['year'][0])

            # --- Album -----------------------------
            d.setdefault('album', f['metadata']['songs']['release'][0])

            # --- Similar Artist -----------------------------
            d.setdefault('a_similar', np.array(f['metadata']['similar_artists']))

            # --- Artist Terms -----------------------------
            d.setdefault('a_terms', np.array(f['metadata']['artist_terms']))
            d.setdefault('a_tfrq', np.array(f['metadata']['artist_terms_freq']))
            d.setdefault('a_tw', np.array(f['metadata']['artist_terms_weight']))

            return d
        else: 
            pass

def get_json_info(path):
    with open(path) as data_file:    
        return json.load(data_file)

def makeCSVline(line):
    return ','.join(str(line[f]) for f in fieldsBrC.value)
    
def artistToTags(record):
    '''
    Concatenate artist with each tag
    Normalize tag frequency and weight
    '''
    normalize_frq = record['a_tfrq'] / sum(record['a_tfrq'])
    normalize_w = record['a_tw'] / sum(record['a_tw'])
    terms = record['a_terms']
    artist = record['artist_id']
    
    result = []
    for i in range(len(terms)):
        result.append( artist +","+ terms[i] +","+ str(normalize_frq[i]) +","+ str(normalize_w[i]))
    
    return result

def songToTags(record):
    '''
    Concatenate song with each tag
    '''
    tags = record['tags']
    total_weight = sum(float(w[1]) for w in tags)
    track_id = record['track_id']
    
    result = []
    for i in range(len(tags)):
        if (tags[i][0] <> None) and (tags[i][0] <> ''):
            result.append( track_id +","+ tags[i][0] +","+ str(float(tags[i][1])/total_weight))
    
    return result

if __name__ == '__main__':
    '''
    input_path: path to where the list of hdf5 and json files was created
    output_path: a temporary directory where the Spark CSV files separated as part-000xx files will be stored
    mismatch_path: path to where the mismatches file is located
    
    DO NOT INCLUDE '/' AT THE END OF PATH
    Cannot change file names
    '''
    
    inDir = sys.argv[1]  
    outDir = sys.argv[2]
    mismatch_path = sys.argv[3]
    cpus = int(sys.argv[4])
    
    #main(input_path, output_path)
    # === Start Spark Context ===
    sc = SparkContext(appName="SparkProcessing")
    
    # =====================================================================
    # === Load mismatches ===
    toRemoveRDD = sc.textFile('file://'+mismatch_path+'/sid_mismatches.txt',cpus).map(parse_mismatches)
    #songsToRemove = toRemoveRDD.collect()
    songsToRemove = sc.broadcast(toRemoveRDD.collect())
    
    
    # =====================================================================
    # === Load list of files ====== Extract Song Data ===
    song_pathsRDD   = sc.textFile('file://' + inDir + '/list_hdf5_files.txt',cpus)
    lastfm_pathsRDD = sc.textFile('file://' + inDir + '/list_lastfm_files.txt',cpus)
    
    # === Extract Song Data ===
    songsRDD = song_pathsRDD.map(get_h5_info).filter(lambda x: x<>None).cache()
    
    
    # =====================================================================
    # == Delete Sub-Folders ===
    folders = ['/nodes_artists','/nodes_songs','/nodes_albums','/nodes_years','/nodes_tags',
              '/rel_similar_artists', '/rel_performs','/rel_artist_has_album','/rel_artist_has_tag',
               '/rel_song_in_album','/rel_similar_songs', '/rel_song_has_tag', '/rel_song_year']
    for p in folders:
        if os.path.exists(outDir+p):
            shutil.rmtree(outDir+p)
    
    
    # === ARTISTS ===
    # CSV Format: artist_id, artist_mb_id, artist_7d_id, artist_name
    fields = ['artist_id', 'artist_mbid', 'artist_7did', 'artist_name']
    fieldsBrC = sc.broadcast(fields)
    songsRDD.map(makeCSVline).distinct().saveAsTextFile('file://'+outDir+'/nodes_artists')
    
    # === SONGS ===
    # CSV Format: song_id, track_id, song_title, danceability, duration, energy, loudness
    fields = ['song_id', 'track_id', 'title', 'dance', 'dur', 'energy','loudness']
    fieldsBrC = sc.broadcast(fields)
    songsRDD.map(makeCSVline).distinct().saveAsTextFile('file://'+outDir+'/nodes_songs')
    
    # === ALBUMS ===
    # CSV Format: album_name
    songsRDD.map(lambda x: x['album']).distinct().saveAsTextFile('file://'+outDir+'/nodes_albums')
    
    # === YEAR ===
    # CSV Format: year
    songsRDD.map(lambda x: x['year']).filter(
        lambda x: int(x) > 0).distinct().saveAsTextFile('file://'+outDir+'/nodes_years')

    
    # === SIMILAR_TO relationship between artist and artist ===
    # CSV Format: from_artist_id, to_artist_id
    # Similar Artist to Artist (directional, no properties)
    similarArtistsRDD = songsRDD.map(lambda x: (x['artist_id'],x['a_similar'])).flatMapValues(lambda x: x)
    similarArtistsRDD.distinct().map(lambda x: x[0]+","+x[1]).saveAsTextFile('file://'+outDir+'/rel_similar_artists')
    
    # === PERFORMS relationship between artist and song ===
    # CSV Format: artist_id, song_id
    # Artist Performs Song (directional, no properties)
    songsRDD.map(lambda x: x['artist_id']+","+x['track_id']).distinct().saveAsTextFile('file://'+outDir+'/rel_performs')
    
    # === HAS_ALBUM relationship between artist and album  ===
    # CSV Format: artist_id, album_name
    # Artist Has Album (directional, no properties)
    songsRDD.map(lambda x: x['artist_id']+","+x['album']).distinct().saveAsTextFile('file://'+outDir+'/rel_artist_has_album')

    # === HAS_TAG relationship between artist and tags ===
    # CSV Format: artist_id, tag_name, tag_frequency, tag_weight
    # Artist Has Tags (directional, has properties frequency and weight)
    songsRDD.flatMap(artistToTags).distinct().saveAsTextFile('file://'+outDir+'/rel_artist_has_tag')
    
    # === RELEASED_ON relationship between song and year
    # CSV Format: song_id, year
    # Song Released in Year (directional, no properties)
    songsRDD.filter(lambda x: int(x['year'])<>0).map(
        lambda x: x['track_id']+","+str(x['year'])).saveAsTextFile('file://'+outDir+'/rel_song_year')
    
    # === IN_ALBUM relationship between song and album ===
    # CSV Format: song_id, album_name
    # Song In Album (direction, no properties)   
    songsRDD.map(lambda x: x['track_id']+","+x['album']).distinct().saveAsTextFile('file://'+outDir+'/rel_song_in_album')
    
    # === Extract Lastfm Song Data ===
    lastfmRDD = lastfm_pathsRDD.map(get_json_info).cache()
    
    # === TAGS ===
    # CSV Format: tag_name
    artistTags = songsRDD.flatMap(lambda x: x['a_terms']).distinct()
    songTags = lastfmRDD.flatMap(lambda x: x['tags']).map(lambda x: x[0]).distinct()
    allTags = songTags.union(artistTags).distinct()
    allTags.saveAsTextFile('file://'+outDir+'/nodes_tags')
    
    # === SIMILAR_TO relationship between song and song ===
    #CSV Format: from_track_id, to_track_id, similarity_measure
    # Similar Song to Song (directional, with property similarity measure)   
    similarSongsRDD = lastfmRDD.filter(
        lambda x: x['similars']<>[]).map(
        lambda x: (x['track_id'],x['similars'])).flatMapValues(lambda x: x)
    similarSongsRDD.map(
        lambda x: x[0]+","+x[1][0]+","+str(x[1][1])).saveAsTextFile('file://'+outDir+'/rel_similar_songs')
    
    # === HAS_TAG relationship between song and tags
    # CSV Format: track_id, tag_name, tag_weight
    lastfmRDD.flatMap(songToTags).saveAsTextFile('file://'+outDir+'/rel_song_has_tag')
    
    # === Stop Spark Context ===
    sc.stop()