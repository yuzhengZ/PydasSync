#!/usr/bin/python

import os
import sys
import subprocess
import re
import shutil
import hashlib
import getopt
import pydas
# import logging
# 
# from logsetting import getHandler, StreamToLogger
# 
# # create logger
# logger = logging.getLogger('server')
# logger.setLevel(logging.INFO)
# stdout_logger = logging.getLogger('STDOUT')
# stdout_logger.setLevel(logging.INFO)
# stderr_logger = logging.getLogger('STDERR')
# stderr_logger.setLevel(logging.INFO)

#########################################################
#
"""
Functions to process files in Data Server and Synchronize them to Midas
"""
#
#########################################################

def __md5_for_file(file, block_size=8192):
    md5 = hashlib.md5()
    with open(file,'rb') as f: 
        for chunk in iter(lambda: f.read(128 * md5.block_size), b''): 
            md5.update(chunk)
    return md5.hexdigest()

def sanity_check(mode, data_dir, midas_url, midas_user_email, midas_apikey, midas_folder_id):
    """
    Sanity check for input parameters
    """
    if mode not in ('info', 'upload', 'download'):
        print ("Caught a sanity check error: mode %s is not supported! Only 3 modes are supported: info, upload or download." % mode)
        return False
    if os.path.isdir(data_dir):
        if mode == 'download' and (not os.access(data_dir, os.W_OK)):
            print ("Caught a sanity check error: in download mode, write permission is needed for data directory %s !" % data_dir)
            return False
    else:
        print ("Caught a sanity check error: data directory %s does not exist!" % data_dir)
        return False
    try:      
        pydas.login(email=midas_user_email, api_key=midas_apikey, url=midas_url)
        pydas.session.communicator.folder_get(pydas.session.token, midas_folder_id)
    except pydas.exceptions.PydasException as detail:
        print "Caught PydasException: ", detail
        return False
    return True

def synchronize_data(mode, data_dir, midas_url, midas_user_email, midas_apikey, midas_folder_id):
    """
    Synchronize data bwteen data_dir and the given folder in the midas instance
    """
    # walk through data directory in the server
#    pydas.login(email=midas_user_email, api_key=midas_apikey, url=midas_url)
    for data_root, data_sub_folders, data_files in os.walk(data_dir):
#        midas_root = pydas.session.communicator.folder_get(pydas.session.token, midas_folder_id)
        for data_folder in data_sub_folders:
            print "%s has subdirectory %s" % (data_root, data_folder)
        for data_filename in data_files:
            print "%s has file %s, its md5 checksum is %s" % (data_root, data_filename, __md5_for_file(os.path.join(data_root, data_filename)))

class Usage(Exception):
    def __init__(self, msg):
        self.msg = msg
        
def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hm:d:u:e:a:f:", ["help", "mode=", "datadir=", "url=", "email=", "apikey=", "folderid=" ])
    except getopt.error, msg:
        raise Usage(msg)
    mode = ''
    data_dir = ''
    midas_url = ''
    midas_user_email = ''
    midas_apikey = ''
    midas_folder_id = ''
#    log_dir = '/tmp'
    for opt, arg in opts:
        if opt in ('-h', "--help"):
            print 'mSync.py -m (info|upload|download) -d <data_directory> -u <midas_url> -e <midas_user_email>  -a <midas_api_key> -f <midas_folder_id>'
            sys.exit()
        elif opt in ("-m", "--mode"):
            mode = arg.lower()
        elif opt in ("-d", "--datadir"):
            data_dir = arg
        elif opt in ("-u", "--url"):
            midas_url = arg
        elif opt in ("-e", "--email"):
            midas_user_email = arg
        elif opt in ("-a", "--apikey"):
            midas_apikey = arg
        elif opt in ("-f", "--folderid"):
            midas_folder_id = arg

#     # set up normal logger
#     logger.addHandler(getHandler(log_dir.strip()))
# 
#     # log stdout and stderr during the period that received DICOM files are
#     # processed in local disk and uploaded to Midas using Pydas
#     stdout_logger.addHandler(getHandler(log_dir.strip()))
#     out_log = StreamToLogger(stdout_logger, logging.INFO)
#     sys.stdout = out_log
#     stderr_logger.addHandler(getHandler(log_dir.strip()))
#     err_log = StreamToLogger(stderr_logger, logging.ERROR)
#     sys.stderr = err_log

    # sanity check for input parameters
    input_sanity = sanity_check(mode, data_dir, midas_url, midas_user_email, midas_apikey, midas_folder_id)
    
    # synchronize data between data_dir and midas folder
    if input_sanity:
        synchronize_data(mode, data_dir, midas_url, midas_user_email, midas_apikey, midas_folder_id)

if __name__ == "__main__":
    sys.exit(main())
