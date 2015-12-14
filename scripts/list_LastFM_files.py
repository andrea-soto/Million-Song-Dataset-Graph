#!/usr/bin/env python
import os
import glob
import sys
import shutil

def main(inFile, outFile, overwrite = False):
    
    try:
        os.path.exists(inFile)
    except:
        print "Input file: '%s' does not exist"%(inFile)
    else:
        outFile = outFile + '/list_lastfm_files.txt'
        if not os.path.exists(outFile) or overwrite:
            # List all paths of songs
            get_song_paths = glob.glob(inFile+'/*/*/*/*.json')
            
            if not get_song_paths:
                print "No JSON files foung in '%s'"%(inFile)
                print "Check that the file structure under '%s' is /*/*/*/song_files.json"%(inFile)
            else:
                with open(outFile,'w') as f:
                    f.writelines('\n'.join(p for p in get_song_paths))
                    f.close()
                print  "File '%s' successfully created"%(outFile)
    
if __name__ == '__main__':
    '''
    Creates the file 'list_lastfm_files.txt' with the list of HDF5 files
    
    USE:
    python list_MDS_files.py <path to songs> <save list path> <OPTIONAL overwrite>
    
    Paths should NOT include '/' at the end
    If the file already exists, it will not be overwritten. Send 'True' to overwrite
    '''
    input_path = sys.argv[1]  
    output_path = sys.argv[2]
    
    # Option to overwrite current file
    overwrite = False
    if len(sys.argv) > 3:
        overwrite  = sys.argv[3]
    
    main(input_path, output_path, overwrite)