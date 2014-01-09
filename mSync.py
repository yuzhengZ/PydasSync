#!/usr/bin/python
# -*- coding: utf-8 -*-

################################################################################
#
#
# Copyright 2010 Kitware Inc. 28 Corporate Drive,
# Clifton Park, NY, 12065, USA.
#
# All rights reserved.
#
# Licensed under the Apache License, Version 2.0 ( the "License" );
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
################################################################################

"""
mSync: A tool to recursively synchronize data between a local directory and a Midas folder.
"""

import os
import sys
import hashlib
import getopt
import pprint
import pydas

class SyncStatusDict(object):
    """
    Class for synchronization status dictionary 
    """
    def __init__(self):
        self.only_local = {'entire_dirs': [ ], 'files': [ ] }
        self.only_midas = {'entire_folders': [ ], 'items': [ ]}
        self.needs_update = {'files': [ ] }
        
    def is_empty(self):
        if self.only_local['entire_dirs'] or self.only_local['files'] \
            or self.only_midas['entire_folders'] or self.only_midas['items'] \
            or self.needs_update['files']:
            return False
        else:
            return True

    def pprint(self):
        pp = pprint.PrettyPrinter(indent = 2)
        print ("The current synchronization information is as below: ")
        pp.pprint({"only_local" : self.only_local,
                   "only_midas" : self.only_midas,
                   "nedds_update" : self.needs_update})

        
class SyncSetting(object):
    """
    Class for synchronization setting
    """
    def __init__(self, mode, local_root_dir, midas_url, midas_apikey, 
                 midas_user_email, midas_root_folder_id):
        self.mode = mode
        self.local_root_dir = local_root_dir
        self.midas_url = midas_url
        self.midas_apikey = midas_apikey
        self.midas_user_email = midas_user_email
        self.midas_root_folder_id = midas_root_folder_id

        
class Usage(Exception):
    def __init__(self, msg):
        self.msg = msg
        

def _md5_for_file(file, block_size=8192):
    """
    Helper function to calculate the md5 checksum for a local file
    """
    md5 = hashlib.md5()
    with open(file,'rb') as f: 
        for chunk in iter(lambda: f.read(128 * md5.block_size), b''): 
            md5.update(chunk)
    return md5.hexdigest()


def _get_midas_folder_ancestor(midas_folder_id):
    """
    Helper function to get the ancestor list for a Midas folder
    """
    ancestor_list = [ ]
    current_folder_id = midas_folder_id
    while int(current_folder_id) > 0:
        folder_info = pydas.session.communicator.folder_get(
            pydas.session.token, current_folder_id)
        ancestor_list.append(folder_info['name'])
        current_folder_id = folder_info['parent_id']
    id = ancestor_list[-1].split('_', 1)[-1]
    ancestor_list[-1] = id
    if current_folder_id == '-2':
        return 'community',  ancestor_list
    elif current_folder_id == '-1':
        return 'user', ancestor_list


def _get_pydas_server_path(midas_folder_id):
    """
    Helper function to get destination for pydas.upload()
    """
    ancestor_type, destination_list = _get_midas_folder_ancestor(midas_folder_id)
    id = destination_list[-1]
    if ancestor_type == 'community':
        # get community name
        community_info = pydas.session.communicator.get_community_by_id(
                id, pydas.session.token)
        destination_list[-1] = community_info['name']
        destination_list.append('communities')
    elif ancestor_type == 'user':
        # get user name
        user_info = pydas.session.communicator.get_user_by_id(id)
        destination_list[-1] = user_info['firstname'] + '_' + user_info['lastname']
        destination_list.append('users')
    else:
        print "Cannot find the pydas.upload() destination " \
            "for the Midas folder whose id is %s . ", midas_folder_id
        return midas_destination
    midas_destination =  '/' + '/'.join(destination_list[::-1])
    return midas_destination


def _upload_permision_check(data_dir, midas_folder_id):
    """
    Check if the user is allowed to upload data to a Midas folder
    Permission rules: 
        1) a Midas user is allowed to upload data to its own folders
        2) TODO: a Midas user is allowed to upload data to its community folders 
    """
    pydas_user_info = pydas.session.communicator.get_user_by_email(pydas.session.email)
    ancestor_type, ancestor_list = _get_midas_folder_ancestor(midas_folder_id)
    id = ancestor_list[-1]
    # check if it is the user himself
    if ancestor_type == 'user' and id == pydas_user_info['user_id']:
        return True
    # check if the user joins the community
    elif ancestor_type == 'community':
        community_info = pydas.session.communicator.get_community_by_id(
            id, pydas.session.token)
        community_member_group_id = community_info['membergroup_id']
        # TODO: check if the user joins the community via Pydas
        return True
    else:
        return False


def _query_yes_no(question, default="no"):
    """Helper function to ask a yes/no question via raw_input() and return their answer.
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


def sanity_check(sync_setting):
    """
    Sanity check for input parameters
    """
    if sync_setting.mode not in ('check', 'upload', 'download'):
        print "Caught a sanity check error: mode %s is not supported! " \
            ("Only 3 modes are supported: check, upload or download." % \
            sync_setting.mode)
        return False
    if not os.path.isdir(sync_setting.local_root_dir):
        print ("Caught a sanity check error: data directory %s does not exist!" \
              % sync_setting.local_root_dir)
        return False
    elif sync_setting.mode == 'download' and \
        not os.access(sync_setting.local_root_dir, os.W_OK):
        print ("Caught a sanity check error: in download mode, " \
               "write permission is needed for data directory %s !" \
                % sync_setting.local_root_dir)
        return False      
    try:      
        pydas.login(email=sync_setting.midas_user_email, 
            api_key=sync_setting.midas_apikey, url=sync_setting.midas_url)
        pydas.session.communicator.folder_get(pydas.session.token, 
                                              sync_setting.midas_root_folder_id)
        if sync_setting.mode == 'upload':
            return _upload_permision_check(sync_setting.local_root_dir,
                                           sync_setting.midas_root_folder_id)
    except pydas.exceptions.PydasException as detail:
        print "Caught PydasException: ", detail
        return False
    return True


def check_sync_status(sync_setting):
    """
    Check data synchronize status between a local directory and a Mids folder
    """
    print ("Checking synchronization status between local directory %s " \
           "and Midas folder %s. \nPlease be patient. " \
           % (sync_setting.local_root_dir, os.path.join(
            sync_setting.midas_url, 'folder', sync_setting.midas_root_folder_id)))
    # synchronization status
    sync_status = SyncStatusDict()
    # lookup table: local directory name (including path) -> Midas folder id
    midas_folder_ids_lookup = {}
    midas_folder_ids_lookup[sync_setting.local_root_dir] = sync_setting.midas_root_folder_id

    midas_children_folders = { }
    midas_children_items = { }
    # walk through local directory using topdown mode
    for root, dirs, files in os.walk(sync_setting.local_root_dir, topdown=True):
        # ignore hidden directories
        dirs[:] = [d for d in dirs if not d[0] == '.']
        midas_children_folders.clear()
        midas_children_items.clear()

        # for a given local directory (root), get its corresponding Midas folder,
        # then query Midas to get its children folders and items
        if root in midas_folder_ids_lookup.keys():
            for element_type, element_list in pydas.session.communicator.folder_children(
                    pydas.session.token, midas_folder_ids_lookup[root]).iteritems():
                if element_type == 'folders':
                    for midas_folder in element_list:
                        midas_children_folders[midas_folder['name']] = midas_folder
                elif element_type == 'items':
                    for midas_item in element_list:
                        midas_children_items[midas_item['name']] = midas_item
        else:
            dirs[:] = [ ]
            files[:] = [ ]
            if root not in sync_status.only_local['entire_dirs']:
                sync_status.only_local['entire_dirs'].append(root)

        # for a given local directory (root), check its sub directories (dirs)
        for dir_name in dirs:
            local_dir_path = os.path.join(root, dir_name)
            if dir_name in midas_children_folders.keys():
                midas_folder_ids_lookup[local_dir_path] = midas_children_folders[dir_name]['folder_id']
                midas_children_folders[dir_name]['in_local'] = True
            else:
                sync_status.only_local['entire_dirs'].append(local_dir_path)

        # for a given local directory (root), check its files (files)
        for filename in files:
            local_file_path = os.path.join(root, filename)
            if root in sync_status.only_local['entire_dirs']:
                continue
            elif filename not in midas_children_items.keys():
                sync_status.only_local['files'].append(
                    {'filepath': local_file_path, 
                    'midas_upload_folder_id': midas_folder_ids_lookup[root]})
            else:
                midas_children_items[filename]['in_local'] = True
                midas_item_info = pydas.session.communicator.item_get(
                    pydas.session.token, midas_children_items[filename]['item_id'])
                local_file_checksum = _md5_for_file(local_file_path)
                # assumptions for the items in Midas: 
                # 1) use the latest revision for each item 
                # 2) each item only contains one bitstream
                if midas_item_info['revisions'] \
                   and midas_item_info['revisions'][-1]['bitstreams'] \
                   and local_file_checksum  == \
                       midas_item_info['revisions'][-1]['bitstreams'][0]['checksum']:
                        continue
                sync_status.needs_update['files'].append(
                        {'filepath': local_file_path, 
                         'midas_upload_item_id': midas_item_info['item_id']})
      
        # check midas_only entire_folders and items
        for folder_name, folder_info in midas_children_folders.iteritems():
            if 'in_local' not in folder_info.keys() or not folder_info['in_local']:
                sync_status.only_midas['entire_folders'].append(
                    os.path.join(sync_setting.midas_url, 'folder', folder_info['folder_id']))
        for item_name, item_info in midas_children_items.iteritems():
             if 'in_local' not in item_info.keys() or not item_info['in_local']:
                 sync_status.only_midas['items'].append(
                    os.path.join(sync_setting.midas_url, 'item', item_info['item_id']))

    # display synchronization status
    if sync_status.is_empty():
        print "All data are synchronized between the local directory and the Midas folder!"
        return True, sync_status
    else:
        sync_status.pprint()
        return False, sync_status


def mirror_data_to_midas(sync_setting, sync_status):
    """
    Mirror local data to Midas
    """
    print "\nStart mirroring local data to Midas."
    # upload 'local_only' data to Midas
    pydas_root_upload_destination = _get_pydas_server_path(
        sync_setting.midas_root_folder_id)
    for dir in sync_status.only_local['entire_dirs']:
        pydas_upload_destination = os.path.dirname(
            pydas_root_upload_destination + dir.split(sync_setting.local_root_dir)[-1])
        pydas.upload(dir, destination=pydas_upload_destination)
    for file_info in sync_status.only_local['files']:
        filepath = file_info['filepath']
        filename = os.path.basename(filepath)
        upload_folder_id = file_info['midas_upload_folder_id']
        print "Uploading Item from %s" % filepath
        item = pydas.session.communicator.create_item(
            pydas.session.token, filename, upload_folder_id)
        upload_token = pydas.session.communicator.generate_upload_token(
            pydas.session.token, item['item_id'], filename)
        pydas.session.communicator.perform_upload(
            upload_token, filepath, itemid=item['item_id'])
    # upload 'needs_update' data to Midas
    for file_info in sync_status.needs_update['files']:
        filepath = file_info['filepath']
        filename = os.path.basename(filepath)
        upload_item_id = file_info['midas_upload_item_id']
        print "Updating Item from %s" % filepath
        upload_token = pydas.session.communicator.generate_upload_token(
            pydas.session.token, upload_item_id, filename)
        pydas.session.communicator.perform_upload(
            upload_token, filename, itemid=upload_item_id, revision=None, filepath=filepath)
    # process 'midas_only' data
    if sync_status.only_midas['entire_folders'] or sync_status.only_midas['items']:
        agree_to_delete = _query_yes_no("Some folders and/or files only exist in Midas. "  \
            "Do you want to delete them (delete operation cannot be undo)?")
        if agree_to_delete:
            for folder in sync_status.only_midas['entire_folders']:
                pydas.session.communicator.delete_folder(
                    pydas.session.token, os.path.basename(folder))
            for item in sync_status.only_midas['items']:
                pydas.session.communicator.delete_item(
                    pydas.session.token, os.path.basename(item))      
    print ("Data in %s has been mirrored to %s.\n" \
            % (sync_setting.local_root_dir, os.path.join(sync_setting.midas_url, 
               'folder', sync_setting.midas_root_folder_id)))

 
def download_data_to_local(sync_setting):
    """
    Download data from a Midas folder to a local directory
    """
    # currently, data can only be downloaded to an empty local directory
    # TODO: support full data synchronization in download mode
    entries = os.listdir(sync_setting.local_root_dir)
    if len(entries) != 0:
        print ("Caught a sanity check error: in download mode, " \
                "the destination local directory %s is not empty!" % \
                sync_setting.local_root_dir)
        return
    pydas_root_download_source = _get_pydas_server_path(sync_setting.midas_root_folder_id)
    pydas.download(pydas_root_download_source, local_path=sync_setting.local_root_dir)
    print ("Data in %s has been downloaded to %s.\n" \
            % (os.path.join(sync_setting.midas_url, 'folder', 
            sync_setting.midas_root_folder_id)), sync_setting.local_root_dir)

       
def synchronize_data(sync_setting):
    """
    Recursively synchronize data between a local directory and a Midas folder
    """
    sync_done, sync_status = check_sync_status(sync_setting)
    if sync_done:
        return
    if sync_setting.mode == "upload":
        mirror_data_to_midas(sync_setting, sync_status)
        check_sync_status(sync_setting)
    elif sync_setting.mode == "download" and download_data_to_local(sync_setting):
        check_sync_status(sync_setting)

     
def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hm:l:u:e:a:f:", 
            ["help", "mode=", "localdir=", "url=", "email=", "apikey=", "folderid=" ])
    except getopt.error, msg:
        raise Usage(msg)

    # default synchronization setting
    mode = 'check'
    local_root_dir = None
    midas_url = None
    midas_user_email = None
    midas_apikey = None
    midas_root_folder_id = None

    for opt, arg in opts:
        if opt in ('-h', '--help'):
            print "mSync.py -m (check|upload|download) -l <local_root_directory> " \
                "-u <midas_url> -e <midas_user_email>  -a <midas_api_key> -f <midas_root_folder_id>"
            sys.exit()
        elif opt in ('-m', '--mode'):
            mode = arg.lower()
        elif opt in ("-l", "--localdir"):
            local_root_dir = arg.rstrip('/')
        elif opt in ("-u", "--url"):
            midas_url = arg.rstrip('/')
        elif opt in ("-e", "--email"):
            midas_user_email = arg
        elif opt in ("-a", "--apikey"):
            midas_apikey = arg
        elif opt in ("-f", "--folderid"):
            midas_root_folder_id = arg

    # sanity check for input parameters
    for param in [local_root_dir, midas_url, midas_user_email, midas_apikey, 
                  midas_root_folder_id]:
        if param is None:
            print "Caught a sanity check error: At least one required parameter is missing!"
            print "mSync.py --help for more information"
            sys.exit()
    mode = mode.lower()
    midas_url = midas_url.rstrip('/')
    local_root_dir = os.path.abspath(local_root_dir)
    sync_setting = SyncSetting(mode, local_root_dir, midas_url, midas_apikey, 
        midas_user_email, midas_root_folder_id)
    input_sanity = sanity_check(sync_setting)
    
    # synchronize data
    if input_sanity:
        synchronize_data(sync_setting)
            

if __name__ == "__main__":
    sys.exit(main())
