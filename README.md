# mSync

---
mSync, a python tool to recursively synchronize data between a local directory and a Midas folder.

### Requirements

* Install [Python](http://www.python.org/) version 2.6 or later
* Install [Pydas](http://pydas.readthedocs.org/en/latest/intro.html) 0.2.27 or later
* An [enabled](http://www.kitware.com/midaswiki/index.php/Documentation/Latest/User/Administration/ManagePlugins) web api plugin for your Midas3 instance
* Know your Midas api key (Log in to your Midas3 instance -> My Account -> Api tab -> API key column)

# Command line usage for mSync

#### Get usage information
```
python mSync.py -h
```
#### Short option usage
```
python mSync.py [-m (check|upload|download)] -l <local_dir> -u <midas_url> -e <midas_user_email>  -a <midas_api_key> -f <midas_folder_id>
```

#### Long option usage
```
python mSync.py [--mode=(check|upload|download)] --localdir=<local_dir> --url=<midas_url> --email=<midas_user_email>  --apikey=<midas_api_key> --folderid=<midas_folder_id>
```

#### Options
Short option |  Long option |         Argument            | Meaning
-------------|--------------|-----------------------------|---------------------
 -h          | --help       |             N/A             |
 -m          | --mode       | check OR upload OR download | running mode (if this option is not set, check is used as the default mode)
 -l          | --localdir   | local_directory_path      | local directory path (relative or absolute)
 -u          | --email      | midas_url                 | root url of the target Midas instance
 -a          | --apikey     | midas_api_key             | Midas user's api key 
 -f          | --folderid   | midas_folder_id           | target folder id of the Midas instance


#### Example
* Root url of the target Midas is http://msyncexmaple.com/midas
* The Midas user's email is nobody@nowhere.com and his api key is asdfasdfasd2#$fasdf@asdfas
* He wants to mirror the data from local direcotry ../msynctest/data to a folder (folder id is 12) in the target Midas

To check current synchronization status, the short option command line is

```
python mSync.py -m check -l ../msynctest/data -u http://msyncexmaple.com/midas -e nobody@nowhere.com  -a asdfasdfasd2#$fasdf@asdfas -f 12
```
To mirror data from local directory to Midas folder, the long option command line is

```
python mSync.py --mode=upload --localdir=../msynctest/data --url=http://msyncexmaple.com/midas --email=nobody@nowhere.com  --apikey=asdfasdfasd2#$fasdf@asdfas --folderid=12
```

# setMetadata

---
setMetadata, a python tool to read metadata from an excel file and then recursively set metadata to all the items in a Midas folder.
```
### Requirements

* Install [Python](http://www.python.org/) version 2.6 or later
* Install [Pydas](http://pydas.readthedocs.org/en/latest/intro.html) 0.2.27 or later
* Install [Openpyxl](http://pythonhosted.org/openpyxl/index.html) 1.8.2 or later
* An [enabled](http://www.kitware.com/midaswiki/index.php/Documentation/Latest/User/Administration/ManagePlugins) web api plugin for your Midas3 instance
* Know your Midas api key (Log in to your Midas3 instance -> My Account -> Api tab -> API key column)

# Command line usage for mSync

#### Get usage information
```
python setaMetadata.py -h
```
#### Short option usage
```
python setaMetadata.py -x <metadata_excel_file_path> [-s <excel_sheet_name>] -u <midas_url> -e <midas_user_email>  -a <midas_api_key> -f <midas_folder_id>
```

#### Long option usage
```
python setaMetadata.py --excelfile=<metadata_excel_file_path> [--sheetname=<excel_sheet_name>] --url=<midas_url> --email=<midas_user_email>  --apikey=<midas_api_key> --folderid=<midas_folder_id>
```

#### Options
Short option |  Long option |         Argument            | Meaning
-------------|--------------|-----------------------------|---------------------
 -h          | --help       |             N/A             |
 -x          | --excelfile  | metadata_excel_file_path    | local metadata excel file path (relative or absolute)
 -s          | --sheetname  | excel_sheet_name         | excel sheet name (default: current active sheet)
 -u          | --email      | midas_url                 | root url of the target Midas instance
 -a          | --apikey     | midas_api_key             | Midas user's api key 
 -f          | --folderid   | midas_folder_id           | target folder id of the Midas instance


#### Example
* Root url of the target Midas is http://msyncexmaple.com/midas
* The Midas user's email is nobody@nowhere.com and his api key is asdfasdfasd2#$fasdf@asdfas
* He wants to set the metadata from the active sheet in a local excel file ../msynctest/meta.xlsx to all the items in a folder (folder id is 12) in the target Midas

To set metadata, the short option command line is

```
python setaMetadata.py -x ../msynctest/meta.xlsx -u http://msyncexmaple.com/midas -e nobody@nowhere.com  -a asdfasdfasd2#$fasdf@asdfas -f 12
```
To set metadata, the long option command line is

```
python mSync.py --mode=upload --excelfile=../msynctest/meta.xlsx --url=http://msyncexmaple.com/midas --email=nobody@nowhere.com  --apikey=asdfasdfasd2#$fasdf@asdfas --folderid=12
```