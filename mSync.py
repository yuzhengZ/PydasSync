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

def _md5_for_file(file, block_size=8192):
    """
    Helper function to get the md5 checksum for a local file
    """
    md5 = hashlib.md5()
    with open(file,'rb') as f: 
        for chunk in iter(lambda: f.read(128 * md5.block_size), b''): 
            md5.update(chunk)
    return md5.hexdigest()

def _get_pydas_upload_destination(midas_folder_id):
    """
    Helper function to get destination for pydas.upload()
    """
    parsed_destination = [ ]
    current_folder_id = midas_folder_id
    while int(current_folder_id) > 0:
        try:
            folder_info = pydas.session.communicator.folder_get(
                pydas.session.token, current_folder_id)
            parsed_destination.append(folder_info['name'])
            current_folder_id = folder_info['parent_id']
        except pydas.exceptions.PydasException as detail:
            print "Caught PydasException: ", detail
            pass
    if current_folder_id == '-2':
        # get community name
        community_id = parsed_destination[-1].split('_', 1)[-1]
        try:
            community_info = pydas.session.communicator.get_community_by_id(
                community_id, pydas.session.token)
            parsed_destination[-1] = community_info['name']
        except pydas.exceptions.PydasException as detail:
            print "Caught PydasException: ", detail
            pass
        parsed_destination.append('communities')
    elif current_folder_id == '-1':
        # get user name
        user_id = parsed_destination[-1].split('_')[-1]
        try:
            user_info = pydas.session.communicator.get_user_by_id(user_id)
            parsed_destination[-1] = user_info['firstname'] + '_' + user_info['lastname']
        except pydas.exceptions.PydasException as detail:
            print "Caught PydasException: ", detail
            pass
        parsed_destination.append('users')
    else:
        print "Cannot find the pydas.upload() destination for the Midas folder whose id is %s . ", midas_folder_id
        return midas_destination
    midas_destination =  '/' + '/'.join(parsed_destination[::-1])
    return midas_destination


def _query_yes_no(question, default="no"):
    """Ask a yes/no question via raw_input() and return their answer.

    "question" is a string that is presented to the user.
    "default" is the presumed answer if the user just hits <Enter>.
        It must be "yes" (the default), "no" or None (meaning
        an answer is required of the user).

    The "answer" return value is one of "yes" or "no".
    """
    valid = {"yes":True,   "y":True,  "ye":True,
             "no":False,     "n":False}
    if default == None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError("invalid default answer: '%s'" % default)

    while True:
        sys.stdout.write(question + prompt)
        choice = raw_input().lower()
        if default is not None and choice == '':
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' "\
                             "(or 'y' or 'n').\n")

def sanity_check(mode, data_dir, midas_url, midas_user_email, midas_apikey, midas_folder_id):
    """
    Sanity check for input parameters
    """
    if mode not in ('check', 'upload', 'download'):
        print ("Caught a sanity check error: mode %s is not supported! Only 3 modes are supported: check, upload or download." % mode)
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

def check_sync_status(local_root_dir, midas_root_folder_id, midas_url):
    """
    Check data synchronize status between local_root_dir and the midas_root_folder in the Midas instance
    """
    print ("Checking synchronization status between local directory %s and Midas folder %s, please be patient. " % (local_root_dir, os.path.join(midas_url, 'folder', midas_root_folder_id)))
    # Lookup Table: Directory name (including path) in data server -> Midas folder id
    midas_folder_id_lookup_table = {}
    midas_folder_id_lookup_table[local_root_dir] = midas_root_folder_id
    # Synchronization status
    sync_status = { 'only_midas' : {'entire_folders': [ ],
                                    'items': [ ] },
                    'only_local' : {'entire_dirs': [ ], 
                                    'files': [ ] },
                    'needs_update': {'files': [ ] }
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
            for element_type, element_list in pydas.session.communicator.folder_children(
                    pydas.session.token, midas_folder_id_lookup_table[root]).iteritems():
                if element_type == 'folders':
                    for midas_folder in element_list:
                        midas_children_folders[midas_folder['name']] = midas_folder
                elif element_type == 'items':
                    for midas_item in element_list:
                        midas_children_items[midas_item['name']] = midas_item
        else:
            dirs[:] = [ ]
            files[:] = [ ]
            if root not in sync_status['only_local']['entire_dirs']:
                sync_status['only_local']['entire_dirs'].append(root)
        # For a given local directory (root), check its sub directories (dirs)
        for dir_name in dirs:
            local_dir_path = os.path.join(root, dir_name)
            if dir_name in midas_children_folders.keys():
                midas_folder_id_lookup_table[local_dir_path] = midas_children_folders[dir_name]['folder_id']
                midas_children_folders[dir_name]['in_local'] = True
            else:
                sync_status['only_local']['entire_dirs'].append(local_dir_path)

        # For a given local directory (root), check its files (files)
        for filename in files:
            local_file_path = os.path.join(root, filename)
            if root in sync_status['only_local']['entire_dirs']:
                continue
            elif filename not in midas_children_items.keys():
                sync_status['only_local']['files'].append({'filepath': local_file_path, 
                                                           'midas_upload_folder_id': midas_folder_id_lookup_table[root]})
            else:
                midas_children_items[filename]['in_local'] = True
                midas_item_info = pydas.session.communicator.item_get(
                    pydas.session.token, midas_children_items[filename]['item_id'])
                local_file_checksum = _md5_for_file(local_file_path)
                # Assumptions for the items in Midas: 1) use the latest revision for each item 
                #                                     2) each item only contains one bitstream
                midas_bitstream_checksum = ''
                try:
                    midas_bitstream_checksum = midas_item_info['revisions'][-1]['bitstreams'][0]['checksum']
                except Exception:
                    pass
                if local_file_checksum != midas_bitstream_checksum:
                    sync_status['needs_update']['files'].append({'filepath': local_file_path, 
                                                                 'midas_upload_item_id': midas_item_info['item_id']},
                                                                 )       
        # Check midas_only folders and items
        for folder_name, folder_info in midas_children_folders.iteritems():
            if 'in_local' not in folder_info.keys() or not folder_info['in_local']:
                sync_status['only_midas']['entire_folders'].append(
                    os.path.join(midas_url, 'folder', folder_info['folder_id']))
        for item_name, item_info in midas_children_items.iteritems():
             if 'in_local' not in item_info.keys() or not item_info['in_local']:
                 sync_status['only_midas']['items'].append(
                    os.path.join(midas_url, 'item', item_info['item_id']))
    return sync_status

def display_sync_status(local_root_dir, midas_root_folder_id, midas_url, sync_status):
    """
    Display synchronize status
    """
    # Print synchronization status   
    print ("The current synchronization information between %s and %s is as below: " % (local_root_dir, os.path.join(midas_url, 'folder', midas_root_folder_id)))
    pp = pprint.PrettyPrinter(indent = 2)
    pp.pprint(sync_status)
    
def mirror_data_to_midas(local_root_dir, midas_root_folder_id, midas_url, sync_status):
    """
    Upload data to Midas
    """
    # Upload 'local_only' data to Midas
    pydas_root_upload_destination = _get_pydas_upload_destination(midas_root_folder_id)
    for dir in sync_status['only_local']['entire_dirs']:
        pydas_upload_destination = os.path.dirname(pydas_root_upload_destination + dir.split(local_root_dir)[-1])
        pydas.upload(dir, destination=pydas_upload_destination)
    for file_info in sync_status['only_local']['files']:
        filepath = file_info['filepath']
        filename = os.path.basename(filepath)
        upload_folder_id = file_info['midas_upload_folder_id']
        print 'Uploading Item from %s' % filepath
        item = pydas.session.communicator.create_item(pydas.session.token, filename, upload_folder_id)
        upload_token = pydas.session.communicator.generate_upload_token(pydas.session.token, item['item_id'], filename)
        pydas.session.communicator.perform_upload(upload_token, filepath, itemid=item['item_id'])
    # Upload 'needs_update' data to Midas
    for file_info in sync_status['needs_update']['files']:
        filepath = file_info['filepath']
        filename = os.path.basename(filepath)
        upload_item_id = file_info['midas_upload_item_id']
        print 'Updating Item from %s' % filepath
        upload_token = pydas.session.communicator.generate_upload_token(pydas.session.token, upload_item_id, filename)
        pydas.session.communicator.perform_upload(upload_token, filename, itemid=upload_item_id, revision=None, filepath=filepath)   
    # Process 'midas_only' data
    if sync_status['only_midas']['entire_folders'] or sync_status['only_midas']['items']:
        agree_to_delete = _query_yes_no("Some folders and/or files only exist in Midas, do you want to delete them (delete operation cannot be undo)?")
        if agree_to_delete:
            for folder in sync_status['only_midas']['entire_folders']:
                pydas.session.communicator.delete_folder(pydas.session.token, os.path.basename(folder))
            for item in sync_status['only_midas']['items']:
                pydas.session.communicator.delete_item(pydas.session.token, os.path.basename(item))             
    print ("Data in %s has been synchronized to %s . " % (local_root_dir, os.path.join(midas_url, 'folder', midas_root_folder_id)))
        
def synchronize_data(mode, local_root_dir, midas_root_folder_id, midas_url):
    """
    Synchronize data between local_root_dir and the midas_root_folder in the Midas instance
    """ 
    sync_status = check_sync_status(local_root_dir, midas_root_folder_id, midas_url)
    display_sync_status(local_root_dir, midas_root_folder_id, midas_url, sync_status)
    if mode == "upload":
         mirror_data_to_midas(local_root_dir, midas_root_folder_id, midas_url, sync_status)    

class Usage(Exception):
    def __init__(self, msg):
        self.msg = msg
        
def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hm:d:u:e:a:f:", ["help", "mode=", "datadir=", "url=", "email=", "apikey=", "folderid=" ])
    except getopt.error, msg:
        raise Usage(msg)
    mode = 'upload'
    local_root_dir = '/Users/yuzheng/Source/NeuroDevData'
    midas_url = 'http://localhost/midas/'
    midas_user_email = 'yuzheng.zhou@kitware.com'
    midas_apikey = 'yhcWxg/VyYdCLT5kN9vnWwOg/JsTfNtD'
    midas_root_folder_id = '12'
#    log_dir = '/tmp'
    for opt, arg in opts:
        if opt in ('-h', '--help'):
            print 'mSync.py -m (check|upload|download) -d <local_root_directory> -u <midas_url> -e <midas_user_email>  -a <midas_api_key> -f <midas_root_folder_id>'
            sys.exit()
        elif opt in ('-m', '--mode'):
            mode = arg
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
    mode = mode.lower()
    local_root_dir = local_root_dir.rstrip('/')
    midas_url = midas_url.rstrip('/')
    input_sanity = sanity_check(mode, local_root_dir, midas_url, midas_user_email, midas_apikey, midas_root_folder_id)
    
    # synchronize data between data_dir and midas folder
    if input_sanity:
        synchronize_data(mode, local_root_dir, midas_root_folder_id, midas_url)
            

if __name__ == "__main__":
    sys.exit(main())
