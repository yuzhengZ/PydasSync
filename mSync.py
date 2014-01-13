#!/usr/bin/python

import os
import sys
import subprocess
import re
import shutil
import hashlib
import getopt
import pprint
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

def synchronize_data(mode, local_root_dir, midas_root_folder_id, midas_url):
    """
    Synchronize data between local_root_dir and the midas_root_folder in the Midas instance
    """
    # Lookup Table: Directory name (including path) in data server -> Midas folder id
    midas_folder_id_lookup_table = {}
    midas_folder_id_lookup_table[local_root_dir] = midas_root_folder_id
    # Synchronization status
    sync_status = { "only_midas" : {"entire_folders": [ ],
                                    "items": [ ] },
                    "only_local" : {"entire_dirs": [ ], 
                                    "files": [ ] },
                    "needs_update": {"files": [ ] }
                  }
    
    midas_children_folders = { }
    midas_children_items = { }
    # walk through local root directory
    for root, dirs, files in os.walk(local_root_dir, topdown=True):
        # Ignore hidden directories and files
        dirs[:] = [d for d in dirs if not d[0] == '.']
        files = [f for f in files if not f[0] == '.']
        midas_children_folders.clear()
        midas_children_items.clear()
        # For a given local directory (root), query Midas to get its children folders and items
        if root in midas_folder_id_lookup_table.keys():
            for element_type, element_list in pydas.session.communicator.folder_children(pydas.session.token, midas_folder_id_lookup_table[root]).iteritems():
                if element_type == 'folders':
                    for midas_folder in element_list:
                        midas_children_folders[midas_folder['name']] = midas_folder
                elif element_type == 'items':
                    for midas_item in element_list:
                        midas_children_items[midas_item['name']] = midas_item
        else:
            dirs[:] = [ ]
            files[:] = [ ]
            if root not in sync_status["only_local"]["entire_dirs"]:
                sync_status["only_local"]["entire_dirs"].append(root)
        # For a given local directory (root), check its sub directories (dirs)
        for dir_name in dirs:
            local_dir_path = os.path.join(root, dir_name)
            if dir_name in midas_children_folders.keys():
                midas_folder_id_lookup_table[local_dir_path] = midas_children_folders[dir_name]['folder_id']
                midas_children_folders[dir_name]['in_local'] = True
            else:
                sync_status["only_local"]["entire_dirs"].append(local_dir_path)

        # For a given local directory (root), check its files (files)
        for filename in files:
            local_file_path = os.path.join(root, filename)
            if root in sync_status["only_local"]["entire_dirs"]:
                continue
            elif filename not in midas_children_items.keys():
                sync_status["only_local"]["files"].append(local_file_path)
            else:
                midas_children_items[filename]['in_local'] = True
                midas_item_info = pydas.session.communicator.item_get(pydas.session.token, midas_children_items[filename]['item_id'])
                local_file_checksum = __md5_for_file(local_file_path)
                # Assumptions for the items in Midas: 1) use the latest revision for each item 
                #                                     2) each item only contains one bitstream
                midas_bitstream_checksum = ''
                try:
                    midas_bitstream_checksum = midas_item_info['revisions'][-1]['bitstreams'][0]['checksum']
                except Exception:
                    pass
                if local_file_checksum != midas_bitstream_checksum:
                    sync_status["needs_update"]["files"].append(local_file_path)       
        # Check midas_only folders and items
        for folder_name, folder_info in midas_children_folders.iteritems():
            if 'in_local' not in folder_info.keys() or not folder_info['in_local']:
                sync_status["only_midas"]["entire_folders"].append(os.path.join(midas_url, 'folder', folder_info['folder_id']))
        for item_name, item_info in midas_children_items.iteritems():
             if 'in_local' not in item_info.keys() or not item_info['in_local']:
                 sync_status["only_midas"]["items"].append(os.path.join(midas_url, 'item', item_info['item_id']))
        
    # Print synchronization status   
    print ("The synchronization information between local directory %s and Midas folder %s is as below: " % (local_root_dir, os.path.join(midas_url, 'folder', midas_root_folder_id)))
    pp = pprint.PrettyPrinter(indent = 2)
    pp.pprint(sync_status)

class Usage(Exception):
    def __init__(self, msg):
        self.msg = msg
        
def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hm:d:u:e:a:f:", ["help", "mode=", "datadir=", "url=", "email=", "apikey=", "folderid=" ])
    except getopt.error, msg:
        raise Usage(msg)
    mode = 'info'
    local_root_dir = '/Users/yuzheng/Source/NeuroDevData'
    midas_url = 'http://localhost/midas'
    midas_user_email = 'yuzheng.zhou@kitware.com'
    midas_apikey = 'yhcWxg/VyYdCLT5kN9vnWwOg/JsTfNtD'
    midas_root_folder_id = '12'
#    log_dir = '/tmp'
    for opt, arg in opts:
        if opt in ('-h', "--help"):
            print 'mSync.py -m (info|upload|download) -d <local_root_directory> -u <midas_url> -e <midas_user_email>  -a <midas_api_key> -f <midas_root_folder_id>'
            sys.exit()
        elif opt in ("-m", "--mode"):
            mode = arg.lower()
        elif opt in ("-l", "--localdir"):
            local_root_dir = arg
        elif opt in ("-u", "--url"):
            midas_url = arg
        elif opt in ("-e", "--email"):
            midas_user_email = arg
        elif opt in ("-a", "--apikey"):
            midas_apikey = arg
        elif opt in ("-f", "--folderid"):
            midas_root_folder_id = arg

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
    input_sanity = sanity_check(mode, local_root_dir, midas_url, midas_user_email, midas_apikey, midas_root_folder_id)
    
    # synchronize data between data_dir and midas folder
    if input_sanity:
        synchronize_data(mode, local_root_dir, midas_root_folder_id, midas_url)

if __name__ == "__main__":
    sys.exit(main())
