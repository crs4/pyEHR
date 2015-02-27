from pyehr.aql.parser import *
from pyehr.ehr.services.dbmanager.drivers.interface import DriverInterface
from pyehr.ehr.services.dbmanager.querymanager.results_wrappers import *
from pyehr.ehr.services.dbmanager.errors import *
from pyehr.utils import *
from itertools import izip
from hashlib import md5
from pyehr.ehr.services.dbmanager.querymanager.results_wrappers import ResultSet,\
    ResultColumnDef, ResultRow
import json
import elasticsearch
import time
import re
import sys


class ElasticSearchDriver(DriverInterface):
    """
    Creates a driver to handle I\O with a ElasticSearch server.
    Parameters:
    hosts - list of nodes we should connect to.
     Node should be a dictionary ({"host": "localhost", "port": 9200}), the entire dictionary
     will be passed to the Connection class as kwargs, or a string in the format of host[:port]
     which will be translated to a dictionary automatically. If no value is given the Connection class defaults will be used.
    transport_class - Transport subclass to use.
    kwargs - any additional arguments will be passed on to the Transport class and, subsequently, to the Connection instances.

    Using the given *host:port* dictionary and, if needed, the database (index in elasticsearch terminology) and the collection
    ( document in elasticsearch terminology)  the driver will contact ES when a connection is needed and will interrogate a
    specific *document* type stored in one *index*  within the server. If no *logger* object is passed to constructor, a new one
    is created.
    *port*, *user* and *password* are not used
    """

    # This map is used to encode\decode data when writing\reading to\from ElasticSearch
    #ENCODINGS_MAP = {'.': '-'}   I NEED TO SEE THE QUERIES
    ENCODINGS_MAP = {}

    def __init__(self, host, database,collection,
                 port=elasticsearch.Urllib3HttpConnection, user=None, passwd=None,
                 index_service=None, logger=None):
        self.client = None
        self.host = host
        #usare port per transportclass ?   self.transportclass
        self.transportclass=port
        #cosa usare per parametri opzionali???? self.others
        self.user = user
        self.passwd = passwd

        # self.client = None
        # self.database = None
        # self.collection = None
        # self.host = host
        #self.database_name = database
        self.database = database
        self.collection = collection
        self.collection_name = collection
        # self.port = port
        # self.user = user
        # self.passwd = passwd
        self.index_service = index_service
        self.logger = logger or get_logger('elasticsearch-db-driver')
        self.regtrue = re.compile("([ :])True([ \]},])")
        self.regfalse = re.compile("([ :])False([ \]},])")


    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.disconnect()
        return None

    def connect(self):
        """
        Open a connection to a ES server.
        """
        if not self.client:
            self.logger.debug('connecting to host %s', self.host)
            try:
                self.client = elasticsearch.Elasticsearch(self.host,connection_class=self.transportclass)
                self.client.info()
            except elasticsearch.TransportError:
                raise DBManagerNotConnectedError('Unable to connect to ElasticSearch at %s:%s' %
                                                (self.host[0]['host'], self.host[0]['port']))
            #self.logger.debug('binding to database %s', self.database_name)
            self.logger.debug('binding to database %s', self.database)
            #self.database = self.client[self.database_name]
            #there is no authentication/authorization layer in elasticsearch
            #if self.user:
            #    self.logger.debug('authenticating with username %s', self.user)
            #    self.database.authenticate(self.user, self.passwd)
            self.logger.debug('using collection %s', self.collection)
            #self.collection = self.database[self.collection_name]
        else:
            #self.logger.debug('Alredy connected to database %s, using collection %s',
            #                  self.database_name, self.collection_name)
            self.logger.debug('Alredy connected to ElasticSearch')

    def disconnect(self):
        """
        Close a connection to a ElasticSearch server.
        There's not such thing so we erase the client pointer
        """
        self.logger.debug('disconnecting from host %s', self.host)
        #self.client.disconnect()
        self.database = None
        self.collection = None
        self.client = None

    def init_structure(self, structure_def):
        # ElasticSearch would benefit from structure initialization but it doesn't need it
        pass

    @property
    def is_connected(self):
        """
        Check if the connection to the ElasticSearch server is opened.

        :rtype: boolean
        :return: True if the connection is open, False if it is closed.
        """
        return not self.client is None

    def __check_connection(self):
        if not self.is_connected:
            raise DBManagerNotConnectedError('Connection to host %s is closed' % self.host)

    def select_collection(self, collection_label):
        """
        Change the collection for the current database

        :param collection_label: the label of the collection that must be selected
        :type collection_label: string

        """
        self.__check_connection()
        self.logger.debug('Changing collection for database %s, old collection: %s - new collection %s',
                          self.database, self.collection, collection_label)
        self.collection = collection_label
        self.collection_name = collection_label

    def _select_doc_type(self,strid):
        self.collection_name = self.collection+"_"+strid

    def _encode_patient_record(self, patient_record):
        encoded_record = {
            'creation_time': patient_record.creation_time,
            'last_update': patient_record.last_update,
            'active': patient_record.active,
            'ehr_records': [str(ehr.record_id) for ehr in patient_record.ehr_records]
        }
        if patient_record.record_id:   #it's always true
            encoded_record['_id'] = patient_record.record_id
        return encoded_record

    def _normalize_keys(self, document, original, encoded):
        normalized_doc = {}
        for k, v in document.iteritems():
            k = k.replace(original, encoded)
            if not isinstance(v, dict):
                if isinstance(v, list):
                    v = [self._normalize_keys(x, original, encoded) for x in v]
                normalized_doc[k] = v
            else:
                normalized_doc[k] = self._normalize_keys(v, original, encoded)
        return normalized_doc

    def _encode_clinical_record(self, clinical_record):
        ehr_data = clinical_record.ehr_data.to_json()
        for original_value, encoded_value in self.ENCODINGS_MAP.iteritems():
            ehr_data = self._normalize_keys(ehr_data, original_value, encoded_value)
        encoded_record = {
            'patient_id': clinical_record.patient_id,
            'creation_time': clinical_record.creation_time,
            'last_update': clinical_record.last_update,
            'active': clinical_record.active,
            'ehr_data': ehr_data,
            '_version': clinical_record.version
        }
        if clinical_record.structure_id:
            encoded_record['ehr_structure_id'] = clinical_record.structure_id
        if clinical_record.record_id:
            encoded_record['_id'] = clinical_record.record_id
        return encoded_record

    def _encode_clinical_record_revision(self, clinical_record_revision):
        ehr_data = clinical_record_revision.ehr_data.to_json()
        for original_value, encoded_value in self.ENCODINGS_MAP.iteritems():
            ehr_data = self._normalize_keys(ehr_data, original_value, encoded_value)
        return {
            '_id': clinical_record_revision.record_id['_id']+"_"+str(clinical_record_revision.record_id['_version']),
            'ehr_structure_id': clinical_record_revision.structure_id,
            'patient_id': clinical_record_revision.patient_id,
            'creation_time': clinical_record_revision.creation_time,
            'last_update': clinical_record_revision.last_update,
            'active': clinical_record_revision.active,
            'ehr_data': ehr_data,
            '_version': clinical_record_revision.record_id['_version'],
            'archived': True
        }

    def encode_record(self, record):
        """
        Encode a :class:`Record` object into a data structure that can be saved within
        ElasticSearch

        :param record: the record that must be encoded
        :type record: a :class:`Record` subclass
        :return: the record encoded as a ElasticSearch document
        """
        from pyehr.ehr.services.dbmanager.dbservices.wrappers import PatientRecord,\
                ClinicalRecord,ClinicalRecordRevision

        if isinstance(record, PatientRecord):
            return self._encode_patient_record(record)
        elif isinstance(record, ClinicalRecordRevision):
            return self._encode_clinical_record_revision(record)
        elif isinstance(record, ClinicalRecord):
            return self._encode_clinical_record(record)
        else:
            raise InvalidRecordTypeError('Unable to map record %r' % record)

    def _decode_patient_record(self, record, loaded):
        from pyehr.ehr.services.dbmanager.dbservices.wrappers import PatientRecord

        record = decode_dict(record)
        if loaded:
            # by default, clinical records are attached as "unloaded"
            ehr_records = [self._decode_clinical_record({'_id': ehr}, loaded=False)
                           for ehr in record['ehr_records']]
            return PatientRecord(
                ehr_records=ehr_records,
                creation_time=record['creation_time'],
                last_update=record['last_update'],
                active=record['active'],
                record_id=record.get('_id'),
            )
        else:
            return PatientRecord(
                creation_time=record['creation_time'],
                record_id=record.get('_id')
            )

    def _decode_keys(self, document, encoded, original):
        normalized_doc = {}
        for k, v in document.iteritems():
            k = k.replace(encoded, original)
            if not isinstance(v, dict):
                normalized_doc[k] = v
            else:
                normalized_doc[k] = self._decode_keys(v, encoded, original)
        return normalized_doc

    def _decode_clinical_record(self, record, loaded):
        from pyehr.ehr.services.dbmanager.dbservices.wrappers import ClinicalRecord,\
            ArchetypeInstance
        record = decode_dict(record)
        if loaded:
            ehr_data = record['ehr_data']
            for original_value, encoded_value in self.ENCODINGS_MAP.iteritems():
                ehr_data = self.decode_keys(ehr_data, encoded_value, original_value)
            crec = ClinicalRecord(
                ehr_data=ArchetypeInstance.from_json(ehr_data),
                creation_time=record['creation_time'],
                last_update=record['last_update'],
                active=record['active'],
                record_id=record.get('_id'),
                structure_id=record.get('ehr_structure_id'),
                version=record['_version']
            )
            if 'patient_id' in record:
                crec._set_patient_id(record['patient_id'])
        else:
            if record.get('ehr_data'):
                arch = ArchetypeInstance(record['ehr_data']['archetype_class'], {})
            else:
                arch = None
            crec =  ClinicalRecord(
                creation_time=record.get('creation_time'),
                record_id=record.get('_id'),
                last_update=record.get('last_update'),
                active=record.get('active'),
                ehr_data=arch,
                structure_id=record.get('ehr_structure_id'),
                version=record.get('_version')
            )
            if 'patient_id' in record:
                crec._set_patient_id(record['patient_id'])
        return crec

    def _decode_clinical_record_revision(self, record):
        from pyehr.ehr.services.dbmanager.dbservices.wrappers import ClinicalRecordRevision, \
            ArchetypeInstance
        record = decode_dict(record)
        ehr_data = record['ehr_data']
        for original_value, encoded_value in self.ENCODINGS_MAP.iteritems():
            ehr_data = self._decode_keys(ehr_data, encoded_value, original_value)
        return ClinicalRecordRevision(
            ehr_data=ArchetypeInstance.from_json(ehr_data),
            patient_id=record['patient_id'],
            creation_time=record['creation_time'],
            last_update=record['last_update'],
            active=record['active'],
            record_id={'_id': record['_id'].rsplit('_', 1)[0],'_revision' : int(record['_id'].rsplit('_', 1)[1])},
            structure_id=record['ehr_structure_id'],
            version=int(record['_id'].rsplit('_', 1)[1]),
        )

    def decode_record(self, record, loaded=True):
        """
        Create a :class:`Record` object from data retrieved from ElasticSearch

        :param record: the ElasticSearch record that must be decoded
        :type record: a ElasticSearch dictionary
        :param loaded: if True, return a :class:`Record` with all values, if False all fields with
          the exception of the record_id one will have a None value
        :type loaded: boolean
        :return: the ElasticSearch document encoded as a :class:`Record` object
        """
        if self._is_clinical_record(record):
            return self._decode_clinical_record(record, loaded)
        else:
            if self._is_clinical_record_revision(record):
                return self._decode_clinical_record_revision(record)
            else:
                return self._decode_patient_record(record, loaded)

    def _is_clinical_record(self,record):
        return ('ehr_data' in record) and ('archived' not in record)

    def _is_clinical_record_revision(self,record):
        return ('ehr_data' in record) and ('archived' in record)

    def _is_patient_record(self,record):
        return 'ehr_data' not in record

    def count(self):
        return self.client.count(index=self.database)['count']

    def count2(self):
        return self.client.search(index=self.database)['hits']['total']

    def add_record(self, record):
        """
        Save a record within ElasticSearch and return the record's ID

        :param record: the record that is going to be saved
        :type record: dictionary
        :return: the ID of the record
        """
        def clinical_add_withid():
            return str(self.client.index(index=self.database,doc_type=self.collection_name,id=record['_id'],
                                version=version,version_type="external",body=record,
                                op_type='create',refresh='true')['_id'])

        def clinical_add_withoutid():
            return str(self.client.index(index=self.database,doc_type=self.collection_name,body=record,
                                             version=version,version_type="external",
                                             op_type='create',refresh='true')['_id'])

        self.__check_connection()
        try:
            if self._is_patient_record(record):
                if(record.has_key('_id')):
                    return str(self.client.index(index=self.database,doc_type=self.collection,id=record['_id'],
                                                 body=record,op_type='create',refresh='true')['_id'])
                else:
                    return str(self.client.index(index=self.database,doc_type=self.collection,body=record,
                                             op_type='create',refresh='true')['_id'])
            else:
                self._select_doc_type(record['ehr_structure_id'])
                version=1
                if(record.has_key('_version')):
                    version=record['_version']
                if self._is_clinical_record(record):
                    if(record.has_key('_id')):
                        return clinical_add_withid()
                    else:
                        return clinical_add_withoutid()
                else: #clinical_revision_record
                    idreturned=clinical_add_withid()
                    return {idreturned.rsplit('_', 1)[0],idreturned.rsplit('_', 1)[1]}
        except elasticsearch.ConflictError:
            raise DuplicatedKeyError('A record with ID %s already exists' % record['_id'])


    def pack_record(self,records):
        #the records must be of the same type!
        #all patient records or all clinical records
        rectype_clinical = True
        if self._is_patient_record(records[0]):
            rectype_clinical= False
        first="{\"create\":{\"_index\":\""+self.database
        puzzle=""
        for dox in records:
            if(rectype_clinical and self._is_patient_record(dox)):
                raise InvalidRecordTypeError("Patient Record among Clinical Records")
            if((not rectype_clinical) and (not self._is_patient_record(dox))):
                raise InvalidRecordTypeError("Clinical Record among Patient Records")
            versionstring=""
            if rectype_clinical:
                self._select_doc_type(dox['ehr_structure_id'])
                if("_version" in dox):
                    versionstring=",\"_version\":"+str(dox['_version'])+",\"version_type\" : \"external\""
            puzzle=puzzle+first+"\",\"_type\":\""+self.collection_name+"\""+versionstring
            if(dox.has_key('_id')):
                puzzle = puzzle+",\"_id\":\""+dox['_id']+"\"}}\n{"
            else:
                puzzle=puzzle+"}}\n{"
            for k in dox:
                if(isinstance(dox[k],str)):
                    puzzle=puzzle+"\""+k+"\":\""+str(dox[k])+"\","
                else:
                    if isinstance(dox[k],dict):
                        puzzle=puzzle+"\""+k+"\":"+str(dox[k]).replace("'","\"")+","
                    else:
                        puzzle=puzzle+"\""+k+"\":"+str(dox[k])+","
            puzzle=puzzle.strip(",")+"}\n"
            puzzle=self.regtrue.sub("\\1true\\2",puzzle)
            puzzle=self.regfalse.sub("\\1false\\2",puzzle)
        return puzzle

    def add_records(self, records,skip_existing_duplicated=False):
         """
         Save a list of records within ElasticSearch and return records' IDs

         :param records: the list of records that is going to be saved
         :type record: list
         :return: a list of records' IDs
         :rtype: list
         """
         self.__check_connection()
         #create a bulk list
         bulklist = self.pack_record(records)
         bulkanswer = self.client.bulk(body=bulklist,index=self.database,refresh='true')
         notduplicatedlist=[]
         err=[]
         errtype=[]
         nerrors=0
         if(bulkanswer['errors']): # there are errors
            for b in bulkanswer['items']:
                if(b['create'].has_key('error')):
                    err.append(str(b['create']['_id']))
                    errtype.append(str(b['create']['error']))
                    nerrors += 1
                else:
                    notduplicatedlist.append(b['create']['_id'])
                if(nerrors and not skip_existing_duplicated):
                    raise DuplicatedKeyError('Record with these id already exist: %s \n List of Errors found %s' %(err,errtype) )
            return notduplicatedlist,err
         else:
            return [b['create']['_id'] for b in bulkanswer['items']],[]

    def add_records2(self, records, skip_existing_duplicated=False):
        """
        Save a list of records within ElasticSearch and return records' IDs

        :param records: the list of records that is going to be saved
        :type records: list
        :param skip_existing_duplicated: ignore duplicated key errors and save records with a unique ID
        :type skip_existing_duplicated: bool
        :return: a list of records' IDs and a list of records that caused a duplicated key error
        """
        self._check_batch(records, '_id')
        self.__check_connection()
        return super(ElasticSearchDriver, self).add_records(records, skip_existing_duplicated)

    def get_record_by_id(self, record_id):
        """
        Retrieve a record using its ID

        :param record_id: the ID of the record
        :return: the record of None if no match was found for the given record
        :rtype: dictionary or None

        """
        self.__check_connection()
        #res = self.client.get(index=self.database,id=record_id,_source='true')
        try:
            if(isinstance(record_id,dict)):
                newid=record_id ['_id']['_id']+"_"+str(record_id['_id']['_version'])
                res = self.client.get_source(index=self.database,id=newid)
            else:
                res = self.client.get_source(index=self.database,id=record_id)
            return decode_dict(res)
        except elasticsearch.NotFoundError:
            return None

    def get_record_by_version(self, record_id, version):
        """
        Retrieve a record using its ID and version number

        :param record_id: the ID of the record
        :param version: the version number of the record
        :type version: int
        :return: the record or None if no match was found
        :rtype: dict or None
        """
        self.__check_connection()
        #res = self.client.get(index=self.database,id=record_id,_source='true')
        try:
            #search in ehr archive repository first
            if(isinstance(record_id,dict)):
                newid=record_id['_id']+"_"+str(version)
                res = self.client.get(index=self.database,id=newid,version=version,version_type="external")
                return decode_dict(res['_source'])
            else:
                newid=record_id+"_"+str(version)
                res = self.client.get(index=self.database,id=newid,version=version,version_type="external")
                return decode_dict(res['_source'])
        except (elasticsearch.NotFoundError, elasticsearch.ConflictError,elasticsearch.TransportError) :
                #search in ehr repository
                try:
                    if(isinstance(record_id,dict)):
                        newid=record_id['_id']
                        res = self.client.get(index=self.database,id=newid,version=version,version_type="external")
                        return decode_dict(res['_source'])
                    else:
                        newid=record_id
                        res = self.client.get(index=self.database,id=newid,version=version,version_type="external")
                        return decode_dict(res['_source'])
                except (elasticsearch.NotFoundError, elasticsearch.ConflictError,elasticsearch.TransportError):
                    return None

    def get_revisions_by_ehr_id(self, ehr_id):
        """
        Retrieve all revisions for the given EHR ID

        :param ehr_id: the EHR ID that will be used to retrieve revisions
        :return: all revisions matching given ID
        :rtype: list
        """
        query="{ \"filter\" : { \"prefix\" : { \"_id\" : \""+str(ehr_id)+"_\" } } }"
        return self.get_records_by_query(query)


    def get_all_records(self):
        """
        Retrieve all records within current collection

        :return: all the records stored in the current collection
        :rtype: list
        """
        self.__check_connection()
        query="{ \"filter\" : { \"prefix\" : { \"_type\" : \""+self.collection+"\" } } }"
        restot = self.client.search(index=self.database,body=query)['hits']['hits']
        res = [p['_source'] for p in restot]
        if res != []:
            return ( decode_dict(res[i]) for i in range(0,len(res)) )
        return None

    def get_records_by_value(self, field, value):
        """
        Retrieve all records whose field *field* matches the given value

        :param field: the field used for the selection
        :type field: string
        :param value: the value that must be matched for the given field
        :return: a list of records
        :rtype: list
        """
        myquery = {
            "filter" : {
                    "term" : { field : value }
                    }
                }
        restot = self.client.search(index=self.database,body=myquery)['hits']['hits']
        res = [p['_source'] for p in restot]
        if res != []:
            return ( decode_dict(res[i]) for i in range(0,len(res)) )
        return None

    def delete_record(self, record_id):
        """
        Delete an existing record

        :param record_id: record's ID
        """
        self.__check_connection()
        self.logger.debug('deleting document with ID %s', record_id)
        try:
            if(isinstance(record_id,dict)):
                newid=record_id['_id']['_id']+"_"+str(record_id['_id']['_version'])
                myquery="{\"query\" : { \"term\" : { \"_id\" : \""+newid+"\"}}}"
                self.delete_records_by_query(myquery)
            else:
                myquery="{\"query\" : { \"term\" : { \"_id\" : \""+str(record_id)+"\"}}}"
                self.delete_records_by_query(myquery)
        except elasticsearch.NotFoundError:
            return None

    def delete_later_versions(self, record_id, version_to_keep=0):
        """
        Delete versions newer than version_to_keep for the given record ID.

        :param record_id: ID of the record
        :param version_to_keep: the older version that will be preserved, if 0
                                delete all versions for the given record ID
        :type version_to_keep: int
        :return: the number of deleted records
        :rtype: int
        """
        if(isinstance(record_id,dict)):
            newid = record_id['_id']
            query="{ \"query\" : { \"bool\" : { \"must\" : { \"prefix\" : { \"_id\" : \""+str(newid)+"_"+\
              "\" }}}}}"
        else:
            query="{ \"query\" : { \"bool\" : { \"must\" : { \"prefix\" : { \"_id\" : \""+str(record_id)+"_"+\
              "\" }}}}}"

        res=self.get_records_by_query(query)
        counter=0
        if(res):
            for r in res:
                if(r['_version'] > version_to_keep):
                    self.delete_record(r['_id'])
                    counter=counter+1
        return counter

    def delete_records_by_query(self, query):
        """
        Delete all records that match the given query

        :param query: the query used to select records that will be deleted
        :type query: dict
        :return: the number of deleted records
        :rtype: int
        """
        self.__check_connection()
        try:
            restrue=None
            forrestrue=self.client.search(index=self.database,body=query)
            if forrestrue:
                restrue=forrestrue['hits']['total']
            res=self.client.delete_by_query(index=self.database,body=query)
            self.client.indices.refresh(index=self.database)
            return restrue
        except elasticsearch.NotFoundError:
            return None


    def update_field(self, record_id, field_label, field_value, update_timestamp_label=None,
                     increase_version=False):
        """
        Update record's field *field* with given value

        :param record_id: record's ID
        :param field_label: field's label
        :type field_label: string
        :param field_value: new value for the selected field
        :param update_timestamp_label: the label of the *last_update* field of the record if the last update timestamp
          must be recorded or None
          For ElasticSearch the default is not storing the timestamp
        :type update_timestamp_label: field label or None
        :param increase_version: if True, increase record's version number by 1
        :type increase_version: bool
        :return: the timestamp of the last update as saved in the DB or None (if update_timestamp_field was None)
        """
        record_to_update = self.get_record_by_id(record_id)
        if record_to_update is None:
            self.logger.debug('No record found with ID %r', record_id)
            return None
        else:
            if not self._is_patient_record(record_to_update):
                self._select_doc_type(record_to_update['ehr_structure_id'])
            record_to_update[field_label]= field_value
            if update_timestamp_label:
                last_update = time.time()
                record_to_update['last_update']=last_update
            else:
                last_update=None
            if increase_version:
                newversion = record_to_update['_version']+1
                record_to_update['_version']=newversion
                res = self.client.index(index=self.database,doc_type=self.collection_name,body=record_to_update,id=record_id,\
                                    version=newversion,version_type="external")
                if(self._is_clinical_record_revision(record_to_update)):
                    self.logger.debug('updated %s document',res[u'_id'].rsplit('_', 1)[0])
                else:
                    self.logger.debug('updated %s document', res[u'_id'])
            else:
                if(self._is_clinical_record_revision(record_to_update)):
                    res = self.client.index(index=self.database,doc_type=self.collection_name,body=record_to_update,id=record_id,\
                                    version=record_to_update['_version'],version_type="external")
                    self.logger.debug('updated %s document',res[u'_id'].rsplit('_', 1)[0])
                else:
                    if record_to_update.has_key('_version'):
                        res = self.client.index(index=self.database,doc_type=self.collection_name,body=record_to_update,id=record_id,\
                                    version=record_to_update['_version'],version_type="external")
                    else:
                        res = self.client.index(index=self.database,doc_type=self.collection_name,body=record_to_update,id=record_id)
                    self.logger.debug('updated %s document', res[u'_id'])
            return last_update

    def replace_record(self, record_id, new_record, update_timestamp_label=None):
        """
        Replace record with *record_id* with the given *new_record*

        :param record_id: the ID of the record that will be replaced with the new one
        :param new_record: the new record
        :type new_record: dict
        :param update_timestamp_label: the label of the *last_update* field of the record
                                       if the last update timestamp must be recorded or None
        :type update_timestamp_label: field label or None
        :return: the timestamp of the last update as saved in the DB or None
                 (if *update_timestamp_label* was None)
        """
        self.__check_connection()
        last_update = None
        if update_timestamp_label:
            last_update = time.time()
            new_record[update_timestamp_label] = last_update
        try:
            new_record.pop('_id')
        except KeyError:
            pass
        if(not self._is_patient_record(new_record)):
            self._select_doc_type(new_record['ehr_structure_id'])
        if(isinstance(record_id,dict)):
            newid=record_id['_id']+"_"+str(record_id['_version'])
            new_record['_id']=newid
            version=new_record['_version']
            res = self.client.index(index=self.database,doc_type=self.collection_name,body=new_record,id=record_id,version=version,version_type="external")
        else:
            new_record['_id']=record_id
            version=new_record['_version']
            res = self.client.index(index=self.database,doc_type=self.collection_name,body=new_record,id=record_id,version=version,version_type="external")
        return last_update

    def add_to_list(self, record_id, list_label, item_value, update_timestamp_label=None,
                    increase_version=False):
        """
        Append a value to a list within a document

        :param record_id: record's ID
        :param list_label: the label of the field containing the list
        :type list_label: string
        :param item_value: the item that will be appended to the list
        :param update_timestamp_label: the label of the *last_update* field of the record if the last update timestamp
          must be recorded or None
        :type update_timestamp_label: field label or None
        :return: the timestamp of the last update as saved in the DB or None (if update_timestamp_field was None)
        """
        record_to_update = self.get_record_by_id(record_id)
        list_to_update = record_to_update[list_label]
        list_to_update.append(item_value)
        if update_timestamp_label:
            last_update = time.time()
            record_to_update['last_update'] = last_update
        else:
            last_update = None
        if(not self._is_patient_record(record_to_update)):
            self._select_doc_type(record_to_update['ehr_structure_id'])
        res = self.client.index(index=self.database,doc_type=self.collection_name,body=record_to_update,id=record_id)
        self.logger.debug('updated %s document', res[u'_id'])
        return last_update

    def extend_list(self, record_id, list_label, items, update_timestamp_label,
                    increase_version=False):
        return super(ElasticSearchDriver, self).extend_list(record_id, list_label, items,
                                                            update_timestamp_label, increase_version)

    def remove_from_list(self, record_id, list_label, item_value, update_timestamp_label=None,
                         increase_version=False):
        """
        Remove a value from a list within a document

        :param record_id: record's ID
        :param list_label: the label of the field containing the list
        :type list_label: string
        :param item_value: the item that will be removed from the list
        :param update_timestamp_label: the label of the *last_update* field of the record if the last update timestamp
          must be recorded or None
        :type update_timestamp_label: field label or None
        :return: the timestamp of the last update as saved in the DB or None (if update_timestamp_field was None)
        """
        record_to_update = self.get_record_by_id(record_id)
        list_to_update=record_to_update[list_label]
        list_to_update.remove(item_value)
        if update_timestamp_label:
            last_update = time.time()
            record_to_update['last_update'] = last_update
        else:
            last_update = None
        if(not self._is_patient_record(record_to_update)):
            self._select_doc_type(record_to_update['ehr_structure_id'])
        res = self.client.index(index=self.database,doc_type=self.collection_name,body=record_to_update,id=record_id)
        self.logger.debug('updated %s document', res[u'_id'])
        return last_update

    def _map_operand(self, left, right, operand):
        def cast_right_operand(rigth_operand):
            if rigth_operand.isdigit():
                return int(rigth_operand)
            elif '.' in rigth_operand:
                try:
                    i, d = rigth_operand.split('.')
                    if i.isdigit() and d.isdigit():
                        return float(rigth_operand)
                except ValueError:
                    pass
            return rigth_operand

        # map an AQL operand to the equivalent MongoDB one
        operands_map = {
            '>': 'gt',
            '>=': 'gte',
            '<': 'lt',
            '<=': 'lte'
        }
        right = cast_right_operand(right.strip())
        left = left.strip()
        if operand == '=':
            return {"{\"match\" : "+str({left : right})+"}" : "$%nothing%$"}
        elif operand == "!=":
            return   { "  \"must_not\" : { \"match\" : "+str({left : right})+"}" : "$%nothing%$" }
        elif operand in operands_map:
            return { "{ \"range\" :  "+str({left: {operands_map[operand]: right}})+"}" : "$%nothing%$" }
        else:
            raise ValueError('The operand %s is not supported' % operand)

    def _parse_expression(self, expression):
        # replace all invalid characters
        for not_allowed_char, allowed_char in self.ENCODINGS_MAP.iteritems():
            expression = expression.replace(not_allowed_char, allowed_char)
        q = expression.replace('/', '.')
        return q

    def _parse_simple_expression(self, expression):
        return super(ElasticSearchDriver, self)._parse_simple_expression(expression)

    def _parse_match_expression(self, expr):
        return super(ElasticSearchDriver, self)._parse_match_expression(expr)

    def _normalize_path(self, path):
        for original_value, encoded_value in self.ENCODINGS_MAP.iteritems():
            path = path.replace(original_value, encoded_value)
        if path.startswith('/'):
            path = path[1:]
        for x, y in [('[', '/'), (']', ''), ('/', '.')]:
            path = path.replace(x, y)
        return path

    def _build_path(self, path):
        path = list(path)
        path[0] = 'ehr_data'
        tmp_path = '.archetype_details.'.join([self._normalize_path(x) for x in path])
        return '%s.archetype_details' % tmp_path

    def _build_paths(self, aliases):
        return super(ElasticSearchDriver, self)._build_paths(aliases)

    def _extract_path_alias(self, path):
        return super(ElasticSearchDriver, self)._extract_path_alias(path)

    def _get_archetype_class_path(self, path):
        path_pieces = path.split('.')
        path_pieces[-1] = 'archetype_class'
        return '.'.join(path_pieces)

    def _calculate_condition_expression(self, condition, aliases):
        queries = list()
        paths = self._build_paths(aliases)
        for a in izip(*paths.values()):
            for p in [dict(izip(paths.keys(), a))]:
                query = dict()
                # bind fields to archetypes
                for k in aliases.keys():
                    query.update({"\"must\" :{ \"match\" : "+str({self._get_archetype_class_path(p[k]): aliases[k]['archetype_class']})+"}" : "$%nothing%$" })
                expressions = dict()
                or_indices = list()
                and_indices = list()
                for i, cseq_element in enumerate(condition.condition.condition_sequence):
                    if isinstance(cseq_element, PredicateExpression):
                        l_op_var, l_op_path = self._extract_path_alias(cseq_element.left_operand)
                        expressions[i] = self._map_operand('%s.%s' % (p[l_op_var],
                                                                      self._normalize_path(l_op_path)),
                                                           cseq_element.right_operand,
                                                           cseq_element.operand)
                    elif isinstance(cseq_element, ConditionOperator):
                        if cseq_element.op == 'OR':
                            or_indices.extend([i-1, i+1])
                        elif cseq_element.op == 'AND':
                            if (i-1) not in or_indices:
                                and_indices.extend([i-1, i+1])
                            else:
                                and_indices.append(i+1)
                if len(or_indices) > 0:
                    for j in or_indices:
                        if(str(expressions[j]).find("must_not") != -1):
                            exprstr=self._clean_piece(expressions[j])
                            exprstr2="{ \"bool\":"+exprstr+"}"
                            exprstr3=exprstr2.replace("\\'","'").replace("\"bool\":{'","\"bool\":{ {'").replace("}':","}}':")
                            expressions[j]= { exprstr3 : "$%nothing%$"}
                    or_statement= " \"should\" : "+str([ expressions[i] for i in or_indices])+",\"minimum_should_match\" : 1"
                    expressions[max(expressions.keys()) + 1] = or_statement
                    and_indices.append(max(expressions.keys()))
                if len(and_indices) > 0:
                    for i in and_indices:
                        if(isinstance(expressions[i],str)):
                            query.update({str(expressions[i]) : "$%nothing%$" })
                        else:
                            if( str(expressions[i]).find("must_not") == -1 ):
                                query.update({ " \"must\" : "+str(expressions[i]) : "$%nothing%$" })
                            else:
                                query.update(expressions[i])
                else:
                    for e in expressions.values():
                        if (str(e).find("must_not") == -1 ):
                            query.update({" \"must\" : "+ str(e) : "$%nothing%$" })
                        else:
                            query.update(e)
                # append query and used mapping
                if not (query, p) in queries:
                    queries.append((query, p))
        return queries

    def _compute_predicate(self, predicate):
        query = dict()
        if type(predicate) == Predicate:
            pred_ex = predicate.predicate_expression
            if pred_ex:
                lo = pred_ex.left_operand
                if not lo:
                    raise PredicateException("No left operand found")
                op = pred_ex.operand
                ro = pred_ex.right_operand
                if op and ro:
                    self.logger.debug("lo: %s - op: %s - ro: %s", lo, op, ro)
                    if op == "=":
                        query.update({"{ \"match\" : {"+ str({lo : ro})+"}}" : "$%nothing%$" })
#                        query[lo] = ro
            else:
                raise PredicateException("No predicate expression found")
        elif type(predicate) == ArchetypePredicate:
            predicate_string = predicate.archetype_id
            query.update({"{ \"filter\" : {\"exists\" : { \"field\" : "+ str(predicate_string)+"}}}" : "$%nothing%$"} )
        else:
            raise PredicateException("No predicate expression found")
        return query

    def _calculate_ehr_expression(self, ehr_class_expression, query_params, patients_collection,
                                  ehr_collection):
        # Resolve predicate expressed for EHR AQL expression
        query = dict()
        if ehr_class_expression.predicate:
            pr = ehr_class_expression.predicate.predicate_expression
            if pr.left_operand:
                if pr.right_operand.startswith('$'):
                    try:
                        right_operand = query_params[pr.right_operand]
                    except KeyError, ke:
                        raise PredicateException('Missing value for parameter %s' % ke)
                else:
                    right_operand = pr.right_operand
                if pr.left_operand == 'uid':
                    query.update({ " \"must\" : { \"term\" : {\"patient_id\": \"" + str(right_operand).lower() +"\"}}" : "$%nothing%$"})
                elif pr.left_operand == 'id':
                    # use given EHR ID
                    query.update(self._map_operand(pr.left_operand,
                                                   right_operand,
                                                   pr.operand))
                else:
                    query.update(self._compute_predicate(pr))
            else:
                raise PredicateException('No left operand in predicate')
        return query

    def _calculate_location_expression(self, location, query_params, patients_collection,
                                       ehr_collection):
        query = dict()
        # Here is where the collection has been chosen according to the selection
        self.logger.debug("LOCATION: %s", str(location))
        ehr_alias = None
        if location.class_expression:
            ce = location.class_expression
            if ce.class_name.upper() == 'EHR':
                query.update(self._calculate_ehr_expression(ce, query_params,
                                                            patients_collection,
                                                            ehr_collection))
                ehr_alias = ce.variable_name
            else:
                if ce.predicate:
                    query.update(self._compute_predicate(ce.predicate))
        else:
            raise MissiongLocationExpressionError("Query must have a location expression")
        structure_ids, aliases_mapping = self.index_service.map_aql_contains(location.containers)
        if len(structure_ids) == 0:
            return None, None, None
        query.update({"\"must\" : {\"terms\" : {\"ehr_structure_id\" : "+str([structure_ids])+",\"execution\" : \"or\" } }" : "$%nothing%$"})
        return query, aliases_mapping, ehr_alias

    def _map_ehr_selection(self, path, ehr_var):
        path = path.replace('%s.' % ehr_var, '')
        if path == 'ehr_id.value':
            return {'patient_id': True}
        if path == 'uid.value':
            return {'_id': True}

    def _calculate_selection_expression(self, selection, aliases, ehr_alias):
        query = {'_id': False}
        results_aliases = dict()
        for v in selection.variables:
            path = self._normalize_path(v.variable.path.value)
            if v.variable.variable == ehr_alias:
                q = self._map_ehr_selection(path, ehr_alias)
                query.update(q)
                results_aliases[q.keys()[0]] = v.label or '%s%s' % (v.variable.variable,
                                                                    v.variable.path.value)
            else:
                path = '%s.%s' % (aliases[v.variable.variable], path)
                query[path] = True
                # use alias or ADL path
                results_aliases[path] = v.label or '%s%s' % (v.variable.variable,
                                                             v.variable.path.value)
        return query, results_aliases

    def _split_results(self, query_result):
        for key, value in query_result.iteritems():
            if isinstance(value, dict):
                for k, v in self._split_results(value):
                    yield '{}.{}'.format(key, k), v
            elif isinstance(value, list):
                for element in value:
                    for k, v in self._split_results(element):
                        yield '{}.{}'.format(key, k), v
            else:
                yield key, value

    def get_records_by_query(self, query,**otherfields):
        """
        Retrieve all records for the query given with the otherfields extra parameters

        :param otherfields: the parameters to pass to elasticsearch for the search
        :type otherfield: dictionary
        :param query: the value that must be matched for the given field
        :type query: string
        :return: a list of records
        :rtype: list
        """
        size = 100
        restot = []
        resu = self.client.search(index=self.database,size=size,body=query)['hits']
        number_of_results=resu['total']
        restot.extend(resu['hits'])
        if number_of_results > size:
            for i in range(1, (number_of_results-1)/size+1):
                resu = self.client.search(index=self.database,size=size,from_=i*size,body=query)['hits']
                restot.extend(resu['hits'])
        res = [p['_source'] for p in restot]
        if res != []:
            return ( decode_dict(res[i]) for i in range(0,len(res)) )
        return None

    def _run_aql_query(self, query, fields, aliases, collection):
        self.logger.debug("Running query\n%s\nwith filters\n%s", query, fields)
        rs = ResultSet()
        for path, alias in aliases.iteritems():
            col = ResultColumnDef(alias, path)
            rs.add_column_definition(col)
        if self.is_connected:
            original_collection = self.collection
            close_conn_after_done = False
        else:
            close_conn_after_done = True
        self.connect()
        self.select_collection(collection)
        query_results = self.get_records_by_query(query)
        if close_conn_after_done:
            self.disconnect()
        else:
            self.select_collection(original_collection)
        if query_results:
            for q in query_results:
                record = dict()
                for x in self._split_results(q):
                    record[x[0]] = x[1]
                record_sel_al = self.apply_selection(record,fields)
                rr = ResultRow(record_sel_al)
                rs.add_row(rr)
        return rs

    def apply_selection(self,record,fields):
        q={}
        for f in fields:
            if fields[f] == True:
                if f in record:
                    q.update({f : record[f]})
        return q

    def apply_selection_and_aliases(self,record,fields,aliases):
        q={}
        for f in fields:
            if fields[f] == True:
                if f in record:
                    if f in aliases:
                        q.update({aliases[f] : record[f]})
                    else:
                        q.update({f : record[f]})
        return q


    def build_queries(self, query_model, patients_repository, ehr_repository, query_params=None):
        if not query_params:
            query_params = dict()
        selection = query_model.selection
        location = query_model.location
        condition = query_model.condition
        # TODO: add ORDER RULES and TIME CONSTRAINTS
        queries = []
        location_query, aliases, ehr_alias = self._calculate_location_expression(location, query_params,
                                                                                 patients_repository,
                                                                                 ehr_repository)
        if not location_query:
            return queries, []
        if condition:
            condition_results = self._calculate_condition_expression(condition, aliases)
            for condition_query, mappings in condition_results:
                selection_filter, results_aliases = self._calculate_selection_expression(selection, mappings,
                                                                                         ehr_alias)
                queries.append(
                    {
                        'query': (condition_query, selection_filter),
                        'results_aliases': results_aliases
                    }
                )
        else:
            paths = self._build_paths(aliases)
            for a in izip(*paths.values()):
                for p in [dict(izip(paths.keys(), a))]:
                    q = dict()
                    for k in aliases.keys():
                        q.update({"\"must\" : { \"match\" : "+str({self._get_archetype_class_path(p[k]): aliases[k]['archetype_class']})+"}" : "$%nothing%$" })
                    selection_filter, results_aliases = self._calculate_selection_expression(selection, p,
                                                                                             ehr_alias)
                    queries.append(
                        {
                            'query': (q, selection_filter),
                            'results_aliases': results_aliases
                        }
                    )
        return queries, location_query

    def execute_query(self, query_model, patients_repository, ehr_repository, query_params=None):
        """
        Execute a query parsed with the :class:`pyehr.aql.parser.Parser` object and expressed
        as a :class:`pyehr.aql.model.QueryModel`. If the query is a parametric one, query parameters
        must be passed using the query_params dictionary.

        :param query_model: the :class:`pyehr.aql.parser.QueryModel` obtained when the query
                            is parsed
        :type: :class:`pyehr.aql.parser.QueryModel`
        :param query_params: a dictionary containing query's parameters and their values
        :type: dictionary
        :return: a :class:`pyehr.ehr.services.dbmanager.querymanager.query.ResultSet` object
                 containing results for the given query
        """
        def get_selection_hash(selection):
            sel_hash = md5()
            sel_hash.update(json.dumps(selection))
            return sel_hash.hexdigest()

        query_mappings = list()
        queries, location_query = self.build_queries(query_model, patients_repository, ehr_repository, query_params)
        for x in queries:
            if x not in query_mappings:
                query_mappings.append(x)
        # reduce number of queries based on the selection filter
        selection_mappings = dict()
        filter_mappings = dict()
        for qm in query_mappings:
            selection_key = get_selection_hash(qm['query'][1])
            if selection_key not in selection_mappings:
                selection_mappings[selection_key] = {'selection_filter': qm['query'][1],
                                                     'aliases': qm['results_aliases']}
            q = qm['query'][0]
            filter_mappings.setdefault(selection_key, []).append(q)
        total_results = ResultSet()
        querylist=[]
        for sk, sm in selection_mappings.iteritems():
            q1 = self._clean_piece(filter_mappings[sk][0])
            querylist.append("{ \"query\" : { \"bool\" : "+str(q1)+"}")
            q2 = self._clean_piece(location_query)
            querylist.append("\"filter\" : { \"bool\" : "+str(q2)+"}}")
            qs = ",".join(querylist)
            qtot="{ \"query\" : { \"filtered\" : " + qs+ "}}"
            qtot=self._final_check(qtot)
            results = self._run_aql_query(qtot, sm['selection_filter'], aliases=sm['aliases'],
                                          collection=ehr_repository)
            total_results.extend(results)
            querylist[:] = []
        return total_results

    def _final_check(self,qtot):
        ql=list(qtot)
        ob=0
        oq=0
        for i in ql:
            if i=="{":
                ob=ob+1
            elif i=="}":
                ob=ob-1
            elif i=="[":
                oq=oq+1
            elif i=="]":
                oq=oq-1
        if ob != 0:
            #drop or add parentheses from the right and hopefully it will work
            if ob>0:
                for i in range(ob):
                    ql.append("}")
            else:
                for i in range(ob):
                    ql.pop(len(ql) - 1 - ql[::-1].index("}"))
        if oq !=0:
            #drop or add parentheses from the right and hopefully it will work
            if oq>0:
                for i in range(oq):
                    ql.append("]")
            else:
                for i in range(oq):
                    ql.pop(len(ql) - 1 - ql[::-1].index("]"))
        qtot="".join(ql)
        return qtot



    def _clean_piece(self,piece):
        def cleanp(p):
            newp = str(p)
            newpr=newp[::-1]
            #clean p from nothing
            posnothing=[]
            poscolon=[]
            posparleft=[]
            posparright=[]
            starts=0
            ends = len(newp)
            while starts < ends:
                #find position of string nothing
                positionn = newp.find("$%nothing%$",starts,ends)
                if (positionn == -1):
                    starts=ends
                else:
                    starts=positionn+1
                    posnothing.append(positionn)
                    #find position of left colon
                    positionc=newpr.find(":",len(newpr)-positionn,len(newpr))
                    if( positionc == -1):
                        print "not found colon for a given nothing :D"
                        exit(1)
                    else:
                        positionc=len(newpr)-positionc-1
                        poscolon.append(positionc)
                    #find position of first right parenthesis
                    position=newp.find("}",positionn+10,len(newp))
                    position2=newp.find("]",positionn+10,len(newp))
                    isbrace=True
                    if( position==-1 and position2==-1):
                        print "not found parenthesis for a given nothing :D"
                        exit(1)
                    else:
                        if(position2 != -1):
                            if(position == -1):
                                posparright.append(position2)
                                isbrace=False
                            if(position < position2):
                                posparright.append(position)
                            else:
                                posparright.append(position2)
                                isbrace=False
                        else:
                            posparright.append(position)
                    #find position of corresponding left parenthesis
                    br=0
                    sq=0
                    for g,h in enumerate(newpr[(len(newpr)-positionc):len(newpr)]):
                        if h == "{":
                            if isbrace and br==0:
                                posparleft.append(positionc-g-1)
                                break
                            else:
                                br=br-1
                        elif h == "[":
                            if not isbrace and sq==0:
                                posparleft.append(positionc-g-1)
                                break
                            else:
                                sq=sq-1
                        elif h== "}":
                            br=br+1
                        elif h=="]":
                            sq=sq+1
            newpl = list(newp)
            for i in posparleft:
                newpl[i]=""
                newpl[i+1]=""
            for i in range(len(poscolon)):
                for j in range(poscolon[i]-1,posparright[i]+1):
                    newpl[j]=""
            #clean p from extra braces
            #clean p from extra squares
            br=0
            sq=0
            istart=[]
            iend=[]
            cursor=-1
            for i in range(len(newpl)-1):
                if newpl[i]==newpl[i+1]:
                    if(newpl[i]=="{"):
                        br=2
                        istart.append(i)
                        cursor=len(istart)-1
                    elif(newpl[i]=="}"):
                        if(br==2):
                            iend.append(i)
                            cursor=-1
                        else:
                            br=0
                            if(cursor>-1):
                                istart.pop()
                    elif(newpl[i]=="["):
                        sq=1
                        istart.append(i)
                        cursor=len(istart)-1
                    elif(newpl[i]=="]"):
                        if(sq==1):
                            iend.append(i)
                            cursor=-1
                        else:
                            sq=0
                            if(cursor>-1):
                                istart.pop()
                elif newpl[i]=="{":
                    if (i-1) in istart:
                        pass
                    else:
                        br=0
                elif newpl[i]== "[":
                    if (i-1) in istart:
                        pass
                    else:
                        sq=0
            for i in range(len(istart)):
                newpl[istart[i]]=""
                newpl[iend[i]]=""
            #change all single quotation marks to double
            for i in range(len(newpl)-1):
                if newpl[i]=="\\" and newpl[i+1]=="'":
                    newpl[i]=""
                    newpl[i+1]='"'
                elif newpl[i]=="\\" and newpl[i+1]=="\"":
                    newpl[i]=""
                elif newpl[i] == "'":
                    newpl[i]='"'
                elif newpl[i+1]=="'":
                    newpl[i+1]='"'
            newpn = "".join(newpl)
            return newpn
        newpiece="{"
        for p in piece:
            newpiece=newpiece+cleanp(p)+","
        newpiece=newpiece.strip(",")+"}"
        return newpiece
    def get_selection_hash(self,selection):
        sel_hash = md5()
        sel_hash.update(json.dumps(selection))
        return sel_hash.hexdigest()
