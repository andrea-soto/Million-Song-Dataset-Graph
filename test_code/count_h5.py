#!/usr/bin/env python
from pyspark import SparkContext
import time
import h5py

def read_h5_file(path):
    with h5py.File(path, 'r') as f:
        return f['metadata']['songs']['title'][0]
    
t1 = time.time()
sc = SparkContext(appName="SparkHDF5")
file_paths = sc.textFile('file:///data/asoto/projectW205/data/list_files.txt')

songs = file_paths.map(read_h5_file)
songs.count()
t2 = time.time()
sec = t1-t2
print "Time: %0.2f sec = %.2f min = %.2f h"%(sec,sec/60.0,sec/1440.0)
sc.stop()