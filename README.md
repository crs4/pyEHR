## TravisCI build status

[![Build Status](https://travis-ci.org/crs4/pyEHR.png?branch=develop)](https://travis-ci.org/crs4/pyEHR)


# pyEHR

pyEHR is a a scalable clinical data access layer intended for building data management systems for secondary use of structured heterogeneous biomedical and clinical data.

## Documentation

API Documentation can be found [here](http://pyEHR.readthedocs.org/en/latest/)

## Installation and setup

Download pyEHR and unzip it/clone it in a directory [path_to_pyEHR_dir]


Download basex from:
http://basex.org/

In a shell run:
```sh
{path_to_basex_dir}/bin/basexhttp
```

Install Java 8 and export its path:
```sh 
export JAVA_HOME={path_to_java8}
```
Install python dependencies (for example with pip:
```sh
sudo pip install --pre pybasex
sudo pip install voluptuous
sudo pip install bottle
sudo pip install httplib2
```

### Data Management back-end

pyEHR is compatible with multiple back-end engines (right now MongoDB and Elasticsearch are the ones supported) so you can choose which one to install to run with the pyEHR environment.

#### Elasticsearch
Install Elasticsearch 1.5 or lower.
run:
```sh 
{path_to_elasticsearch}/bin/elasticsearch 
```
python module for elasticsearch:
```sh
sudo pip install -Iv elasticsearch==1.5.0
```
export environment variable with driver configuration:
```sh
export SERVICE_CONFIG_FILE={path_to_config_file}/config_elasticsearch.conf
```
#### Mongodb
Install and run MongoDB.
python module for mongodb:
```sh
sudo pip install pymongo
```
export environment variable with driver configuration:
```sh
export SERVICE_CONFIG_FILE={path_to_config_file}/config_mongodb.conf
```

### pyEHR environment
export path to pybasex and pyEHR:
```sh
export PYTHONPATH=${PYTHONPATH}:{path_to_pyEHR_dir}:{path_to_pybasex_dir}
```
install pyEHR:
```sh
sudo python {path_to_pyEHR_dir}/setup.py install
```

pyEHR is now ready to be used

## USAGE

To create a dataset:
```sh
python  {path_to_pyEHR_dir}/test/misc/test_query_performance/datasets_builder.py --structures_config_file {path_to_structures_config_file}/structures_config_file --datasets_config_file {path_to_datasets_config_file}/datasets_config_file --archetypes_dir {path_to_archetypes_dir} --structures_out_file {path_to_structures_out_file}/structures_out_file --datasets_out_file {path_to_datasets_out_file}/datasets_out_file
```

To load a dataset:
```sh
python {path_to_pyEHR_dir}/test/misc/test_query_performance/datasets_loader.py
--datasets_file {path_to_dataset}/dataset.json --pyEHR_config {path_to_config_file}/config_elasticsearch.conf --parallel_processes 8
```

To query loading the queries from a set of queries:
```sh
python {path_to_pyEHR_dir}/test/misc/test_query_performance/run_queries.py --queries_file 
{path_to_queries_file}/queriesfile.json --query_processes 16 --log_file {path_to_log_file}/logfile.log 
--pyEHR_config {path_to_config_file}/config_elasticsearch.conf  --results_file {path_to_results_file}/resultsfile
```

### FILE EXAMPLES
Example of config file for (SERVICE_CONFIG_FILE environment variable):
 * [config/services.elasticsearch.conf] [first]
 * [config/services.mongodb.conf] [second]
 
Example of queries file: 
* [test/misc/test_query_performance/data/conf/queries_conf.json] [third] 
 
Example of structures_config_file: 
* [test/misc/test_query_performance/data/conf/structures_conf.json] [fourth]

Example of datasets_config_file in  
* [test/misc/test_query_performance/data/conf/patients_dataset_conf.json] [fifth]

Example of archetype dir (with archetypes):  
* [models/json] [sixth]

Dataset of 100 records in:
* https://github.com/crs4/pyEHR/tree/develop/test/misc/test_query_performance/data/datasets

Dataset of 10M records in:
* ftp://ftp.crs4.it/surfer/public/pyEHR_TESTING_DATA/DATASET_CONSTANT_NUMBER_OF_RECORDS/

10 Datasets of 1M records in:
* ftp://ftp.crs4.it/surfer/public/pyEHR_TESTING_DATA/DATASET_CONSTANT_NUMBER_OF_RECORDS/

## TESTS
queries:
```
python test/pyEHR/ehr/services/dbmanager/querymanager/test_querymanager.py
```
version manager:
```
python test/pyEHR/ehr/services/dbmanager/dbservices/test_version_manager.py
```
elasticsearch:
```
python test/pyEHR/ehr/services/dbmanager/drivers/test_elasticsearch.py
```
mongodb:
```
python test/pyEHR/ehr/services/dbmanager/drivers/test_mongo.py
```
dbservices:
```
python test/pyEHR/ehr/services/dbmanager/dbservices/test_dbservices.py
```

REST services:
from ./services/
```
python dbservice.py --config ../config/services.elasticsearch.conf
python test/services/test_dbservice.py
```

[//]: # (These are reference links used in the body of this note and get stripped out when the markdown processor does its job. There is no need to format nicely because it shouldn't be seen. Thanks SO - http://stackoverflow.com/questions/4823468/store-comments-in-markdown-syntax)
 [first]: <https://github.com/crs4/pyEHR/tree/develop/config/services.elasticsearch.conf>
 [second]:  <https://github.com/crs4/pyEHR/tree/develop/config/services.mongodb.conf>
 [third]: <https://github.com/crs4/pyEHR/tree/develop/test/misc/test_query_performance/data/conf/queries_conf.json>
 [fourth]: <https://github.com/crs4/pyEHR/tree/develop/test/misc/test_query_performance/data/conf/structures_conf.json>
 [fifth]: <https://github.com/crs4/pyEHR/tree/develop/test/misc/test_query_performance/data/conf/patients_dataset_conf.json>
 [sixth]: <https://github.com/crs4/pyEHR/tree/develop/models/json
