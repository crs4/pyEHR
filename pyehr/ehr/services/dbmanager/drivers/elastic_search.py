from pyehr.aql.parser import *
from pyehr.ehr.services.dbmanager.drivers.interface import DriverInterface
from pyehr.ehr.services.dbmanager.querymanager.results_wrappers import *
from pyehr.ehr.services.dbmanager.errors import *
from pyehr.utils import *
from itertools import izip
from hashlib import md5
from pyehr.ehr.services.dbmanager.querymanager.results_wrappers import ResultSet,\
    ResultColumnDef, ResultRow
from multiprocessing import Pool

try:
    import simplejson as json
except ImportError:
    import json

import elasticsearch
import time
import re
import sys
import os
#from profilehooks import profile


class MultiprocessQueryRunner(object):

    def __init__(self, host, database, collection,
                 port, user, passwd):
        self.host = host
        self.database = database
        self.collection = collection
        self.collection_name = collection
        self.port = port
        self.user = user
        self.passwd = passwd

    def __call__(self, query_description):
        driver_instance = ElasticSearchDriver(
            self.host, self.database, self.collection_name,
            self.port, self.user, self.passwd
        )
        results = driver_instance._run_aql_query(
            query_description['condition'], query_description['selection'],
            query_description['aliases'], self.collection_name
        )
        return results

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

#    @profile
    def __init__(self, host, database,collection,
                 port=None, user=None, passwd=None,
                 index_service=None, logger=None):
        self.client = None
        self.host = host
        self.database = database
        self.collection = collection
        self.collection_name = collection
        self.port = port
        self.user = user
        self.passwd = passwd
        self.transportclass=elasticsearch.Urllib3HttpConnection
        self.index_service = index_service
        self.logger = logger or get_logger('elasticsearch-db-driver')
        self.regtrue = re.compile("([ :])True([ \]},])")
        self.regfalse = re.compile("([ :])False([ \]},])")
        self.threshold = 1000
        self.insert_timeout = 600
        self.database_ids_suffix="lookup"
        baseidids=self.database.rsplit('_', 1)[0]
        self.database_ids=baseidids+"_"+self.database_ids_suffix
        self.doc_ids="table"
        self.scrolltime="1m"
        self.grbq="scan" # "scan" or "from"

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.disconnect()
        return None

#    @profile
    def connect(self):
        """
        Open a connection to a ES server.
        """
        if not self.client:
            self.logger.debug('connecting to host %s', self.host)
            try:
                self.client = elasticsearch.Elasticsearch(hosts=self.host,connection_class=self.transportclass)
                self.client.info()
            except elasticsearch.TransportError:
                raise DBManagerNotConnectedError('Unable to connect to ElasticSearch at %s:%s' %
                                                (self.host[0]['host'], self.host[0]['port']))
            self.logger.debug('binding to database %s', self.database)
            #there is no authentication/authorization layer in elasticsearch
            self.logger.debug('using collection %s', self.collection)
        else:
            self.logger.debug('Alredy connected to ElasticSearch')

#    @profile
    def disconnect(self):
        """
        Close a connection to a ElasticSearch server.
        There's not such thing so we erase the client pointer
        """
        self.logger.debug('disconnecting from host %s', self.host)
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

#    @profile
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

#    @profile
    def _select_doc_type(self,strid):
        self.collection_name = self.collection+"_"+strid

#    @profile
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

    def _to_json(self,doc):
        return json.dumps(doc)

    def _from_json(self,doc):
        return json.loads('"'+doc+'"')

    def my_decode(self,doc):
#        docnew=self._from_json(doc)
#        return decode_dict(docnew)
        return decode_dict(doc)

#    @profile
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

#    @profile
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
            'version': clinical_record.version
        }
        if clinical_record.structure_id:
            encoded_record['ehr_structure_id'] = clinical_record.structure_id
        if clinical_record.record_id:
            encoded_record['_id'] = clinical_record.record_id
        return encoded_record

#    @profile
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
            'version': clinical_record_revision.record_id['_version'],
            'archived': True
        }

#    @profile
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

#    @profile
    def _decode_patient_record(self, record, loaded):
        from pyehr.ehr.services.dbmanager.dbservices.wrappers import PatientRecord

        record = self.my_decode(record)
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

#    @profile
    def _decode_keys(self, document, encoded, original):
        normalized_doc = {}
        for k, v in document.iteritems():
            k = k.replace(encoded, original)
            if not isinstance(v, dict):
                normalized_doc[k] = v
            else:
                normalized_doc[k] = self._decode_keys(v, encoded, original)
        return normalized_doc

#    @profile
    def _decode_clinical_record(self, record, loaded):
        from pyehr.ehr.services.dbmanager.dbservices.wrappers import ClinicalRecord,\
            ArchetypeInstance
        record = self.my_decode(record)
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
                version=record['version']
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
                version=record.get('version')
            )
            if 'patient_id' in record:
                crec._set_patient_id(record['patient_id'])
        return crec

#    @profile
    def _decode_clinical_record_revision(self, record):
        from pyehr.ehr.services.dbmanager.dbservices.wrappers import ClinicalRecordRevision, \
            ArchetypeInstance
        record = self.my_decode(record)
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

#    @profile
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

#    @profile
    def _is_clinical_record(self,record):
        return ('ehr_data' in record) and ('archived' not in record)

#    @profile
    def _is_clinical_record_revision(self,record):
        return ('ehr_data' in record) and ('archived' in record)

#    @profile
    def _is_patient_record(self,record):
        return 'ehr_data' not in record

    @property
    def documents_count(self):
        """
        Get the number of documents within current collection

        :return: the number of current collection's documents
        :rtype: int
        """
        self.__check_connection()
        query="{ \"query\" : { \"prefix\" : { \"_type\" : \""+self.collection+"\" }}}"
        return self.client.search(index=self.database,search_type='count',body=query)['hits']['total']

#    @profile
    def count(self):
        return self.client.count(index=self.database)['count']

    def count2(self):
        return self.client.search(index=self.database)['hits']['total']

#    @profile
    def add_record(self, record):
        """
        Save a record within ElasticSearch and return the record's ID

        :param record: the record that is going to be saved
        :type record: dictionary
        :return: the ID of the record
        """
        def clinical_add_withid():
            if self._is_id_taken(self.database,record['_id']):
                raise DuplicatedKeyError('A record with ID %s already exists' % record['_id'])
            myid = str(self.client.index(index=self.database,doc_type=self.collection_name,id=record['_id'],
                                body=self._to_json(record),op_type='create',refresh='true',timeout=self.insert_timeout)['_id'])
            self._store_ids(record)
            return myid

        def clinical_add_withoutid():
            myid = str(self.client.index(index=self.database,doc_type=self.collection_name,body=self._to_json(record),
                                op_type='create',refresh='true',timeout=self.insert_timeout)['_id'])
            record['_id']=myid
            self._store_ids(record)
            return myid

        self.__check_connection()
        try:
            if self._is_patient_record(record):
                if(record.has_key('_id')):
                    return clinical_add_withid()
                else:
                    return clinical_add_withoutid()
            else:
                self._select_doc_type(record['ehr_structure_id'])
                if self._is_clinical_record(record):
                    if(record.has_key('_id')):
                        return clinical_add_withid()
                    else:
                        return clinical_add_withoutid()
                else:
                    #clinical_revision_record
                    idreturned=clinical_add_withid()
                    return {idreturned.rsplit('_', 1)[0],idreturned.rsplit('_', 1)[1]}
        except elasticsearch.ConflictError:
             raise DuplicatedKeyError('A record with ID %s already exists' % record['_id'])

    def _get_ids(self,baseid):
        try:
            if self.client.exists(index=self.database_ids,doc_type=self.doc_ids,id=baseid):
                return self.client.get_source(index=self.database_ids,doc_type=self.doc_ids,id=baseid)
            return None
        except elasticsearch.NotFoundError:
            return None
    def _store_ids(self,record):
        if self._is_patient_record(record):
            baseid=record['_id']
        else:
            baseid=record['_id'].rsplit('_', 1)[0]
        ind=self.database
        doct=self.collection_name
        existing_record=self._get_ids(baseid)
#        self._select_lookup_db(self.database)
        if existing_record:
            old_value=existing_record['ids']
            old_value.append([record['_id'],ind,doct])
            existing_record['ids']=old_value
            self.client.index(index=self.database_ids,doc_type=self.doc_ids,id=baseid,
                                        body=existing_record,refresh='true',timeout=self.insert_timeout)
        else:
            existing_record=dict()
            existing_record['ids']=[[record['_id'],ind,doct]]
            self.client.index(index=self.database_ids,doc_type=self.doc_ids,id=baseid,
                                        body=existing_record,op_type='create',refresh='true',timeout=self.insert_timeout)


    def _erase_ids(self,id2e):
#       search for the given id2e otherwise for the baseid
        existing_record=self._get_ids(id2e)
        if existing_record:
            old_value=existing_record['ids']
            new_value=[ov for ov in old_value if ov[0] != id2e]
            if new_value:
                existing_record['ids']=new_value
                self.client.index(index=self.database_ids,doc_type=self.doc_ids,id=id2e,
                                        body=existing_record,refresh='true',timeout=self.insert_timeout)
            else:
                self.client.delete(index=self.database_ids,doc_type=self.doc_ids,id=id2e,refresh='true')
        else:
            baseid=id2e.rsplit('_', 1)[0]
            existing_record=self._get_ids(baseid)
            if existing_record:
                old_value=existing_record['ids']
                new_value=[ov for ov in old_value if ov[0] != id]
                if new_value:
                    existing_record['ids']=new_value
                    self.client.index(index=self.database_ids,doc_type=self.doc_ids,id=baseid,
                                        body=existing_record,refresh='true',timeout=self.insert_timeout)
                else:
                    self.client.delete(index=self.database_ids,doc_type=self.doc_ids,id=baseid,refresh='true')
            else:
                raise MissingRevisionError("A record with ID %s does not exist in archive" % id)

#    @profile
    def pack_record(self,records,rectype_clinical):
        #the records must be of the same type!
        #all patient records or all clinical records
        first="{\"create\":{\"_index\":\""+self.database
        puzzle=""
        for dox in records:
            if(rectype_clinical and self._is_patient_record(dox)):
                raise InvalidRecordTypeError("Patient Record among Clinical Records")
            if((not rectype_clinical) and (not self._is_patient_record(dox))):
                raise InvalidRecordTypeError("Clinical Record among Patient Records")
            if rectype_clinical:
                self._select_doc_type(dox['ehr_structure_id'])
            puzzle=puzzle+first+"\",\"_type\":\""+self.collection_name+"\""
            if(dox.has_key('_id')):
                puzzle = puzzle+",\"_id\":\""+dox['_id']+"\"}}\n"
            else:
                puzzle=puzzle+"}}\n"
            puzzle=puzzle+self._to_json(dox)
            puzzle=puzzle+"\n"
        return puzzle


    def _is_id_taken(self,indextc,idtc,collection_nametc=None):
        if collection_nametc:
            return self.client.exists(index=indextc,doc_type=collection_nametc,id=idtc)
        else:
            return self.client.exists(index=indextc,id=idtc)

#    @profile
    def add_records(self,records,skip_existing_duplicated=False):
        """
        Save a list of records within ElasticSearch and return records' IDs

        :param records: the list of records that is going to be saved
        :type record: list
        :return: a list of records' IDs
        :rtype: list
        """
        if len(records) == 0:
            return [],[]
        self.__check_connection()
        if self._is_patient_record(records[0]):
            rectype_clinical= False
        else:
             rectype_clinical= True
        duplicatedlist=[]
        notduplicatedlist=[]
        duplicatedlistid=[]
        records_map={}
        for r in records:
            myid=r['_id']
            if myid in records_map:
                duplicatedlist.append(r)
                duplicatedlistid.append(myid)
            else:
                records_map[myid]=r
                if(not self._is_patient_record(r)):
                    self._select_doc_type(r['ehr_structure_id'])
                if(self._is_id_taken(self.database,myid)):
                    duplicatedlist.append(r)
                    duplicatedlistid.append(myid)
                else:
                    notduplicatedlist.append(r)
        if duplicatedlist and not skip_existing_duplicated:
            raise DuplicatedKeyError('The following IDs are already in use: %s' % duplicatedlistid)
        #create a bulk list
        bulklist = self.pack_record(notduplicatedlist,rectype_clinical)
        bulkanswer = self.client.bulk(body=bulklist,index=self.database,refresh='true',timeout=self.insert_timeout)
        failuresid=[]
        errtype=[]
        nerrors=0
        successfulid=[]
        if(bulkanswer['errors']): # there are errors
            #manual roll back
            for b in bulkanswer['items']:
                if(b['create'].has_key('error')):
                    failuresid.append(str(b['create']['_id']))
                    errtype.append(str(b['create']['error']))
                    nerrors += 1
                else:
                    successfulid.append(b['create']['_id'])
            for s in successfulid:
                r=records_map[s]
                if rectype_clinical:
                    self._select_doc_type(r['ehr_structure_id'])
                self._store_ids(r)
                self.delete_record(s)
#            duplicatedlist.extend([records_map[e] for e in failuresid])
#            return successfulid,duplicatedlist
            return [],duplicatedlist
        else:
            for r in notduplicatedlist:
                if rectype_clinical:
                    self._select_doc_type(r['ehr_structure_id'])
                self._store_ids(r)
            return [b['create']['_id'] for b in bulkanswer['items']],duplicatedlist

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

#    @profile

    def get_record_by_id(self, record_id):
        """
        Retrieve a record using its ID

        :param record_id: the ID of the record
        :return: the record of None if no match was found for the given record
        :rtype: dictionary or None

        """
        self.__check_connection()
        try:
            if(isinstance(record_id,dict)):
                newid=record_id['_id']['_id']+"_"+str(record_id['_id']['_version'])
                res = self.client.get_source(index=self.database,id=newid)
            else:
                res =self.client.get_source(index=self.database,id=record_id)
            return self.my_decode(res)
        except elasticsearch.NotFoundError:
            return None



    def get_record_by_id2(self, record_id):
        """
        Retrieve a record using its ID

        :param record_id: the ID of the record
        :return: the record of None if no match was found for the given record
        :rtype: dictionary or None

        """
        self.__check_connection()
        try:
            #find in ids
            if isinstance(record_id,dict):
                rid=record_id['_id']+"_"+str(record_id['_version'])
            else:
                rid=record_id
            #search for the id
            existing_record=self._get_ids(rid)
            if existing_record:
                er=existing_record['ids']
                f=[elem for elem in er if elem[0]==rid]
                if f:
                    found=f[0]
                    return self.my_decode(self.client.get_source(index=found[1],doc_type=found[2],id=found[0]))
                else:
                    return None
            else:
                baseid=rid.rsplit('_', 1)[0]
                existing_record=self._get_ids(baseid)
                if existing_record:
                    er=existing_record['ids']
                    f=[elem for elem in er if elem[0]==rid]
                    if not f:
                        return None
                    found=f[0]
                    return self.my_decode(self.client.get_source(index=found[1],doc_type=found[2],id=found[0]))
                else:
                    return None
        except elasticsearch.NotFoundError:
            return None

#    @profile
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
        try:
            #find in ids
            if isinstance(record_id,dict):
                rid=record_id['_id']+"_"+str(record_id['_version'])
            else:
                rid=record_id+"_"+str(version)
            baseid=rid.rsplit('_', 1)[0]
            existing_record=self._get_ids(baseid)
            if existing_record:
                er=existing_record['ids']
                f=[elem for elem in er if elem[0]==rid]
                if not f:
                    #check if ehr has it
                    f2=[elem for elem in er if elem[0]==baseid]
                    if not f2:
                        return None
                    found=f2[0]
                    rec=self.client.get_source(index=found[1],doc_type=found[2],id=found[0])
                    if rec.has_key('version'):
                        if rec['version'] == version:
                            return self.my_decode(rec)
                    return None
                found=f[0]
                rec=self.client.get_source(index=found[1],doc_type=found[2],id=found[0])
                if rec.has_key('version'):
                    if rec['version'] == version:
                        return self.my_decode(rec)
            return None
        except (elasticsearch.NotFoundError, elasticsearch.ConflictError,elasticsearch.TransportError) :
            return None

#    @profile
    def get_revisions_by_ehr_id(self, ehr_id):
        """
        Retrieve all revisions for the given EHR ID

        :param ehr_id: the EHR ID that will be used to retrieve revisions
        :return: all revisions matching given ID
        :rtype: list
        """
        self.__check_connection()
        try:
            #find in ids
            if isinstance(ehr_id,dict):
                rid=ehr_id['_id']+"_"+str(ehr_id['_version'])
            else:
                rid=ehr_id
            baseid=rid.rsplit('_', 1)[0]
            existing_record=self._get_ids(baseid)
            if existing_record:
                er=existing_record['ids']
                ff=[elem for elem in er if elem[0]!=baseid]
                if not ff:
                    return None
                results=[]
                for f in ff:
                    results.append(self.client.get_source(index=f[1],doc_type=f[2],id=f[0]))
                return ( self.my_decode(results[i]) for i in range(0,len(results)) )
            return None
        except (elasticsearch.NotFoundError, elasticsearch.ConflictError,elasticsearch.TransportError) :
            return None

#    @profile
    def get_all_records(self):
        """
        Retrieve all records within current collection.
        For Elasticsearch within current database

        :return: all the records stored in the current collection
        :rtype: list
        """
        self.__check_connection()
        query="{ \"filter\" : { \"match_all\" : {} } }"
        restot = self.client.search(index=self.database,body=query)['hits']['hits']
        res = [p['_source'] for p in restot]
        if res != []:
            return ( self.my_decode(res[i]) for i in range(0,len(res)) )
        return None

#    @profile
    def get_records_by_value(self, field, value):
        """
        Retrieve all records whose field *field* matches the given value

        :param field: the field used for the selection
        :type field: string
        :param value: the value that must be matched for the given field
        :return: a list of records
        :rtype: list
        """
        self.__check_connection()
        myquery = {
            "filter" : {
                    "term" : { field : value }
                    }
                }
        restot = self.client.search(index=self.database,body=myquery)['hits']['hits']
        res = [p['_source'] for p in restot]
        if res != []:
            return ( self.my_decode(res[i]) for i in range(0,len(res)) )
        return None

#    @profile
    def get_records_by_values(self, field, values):
        """
        Retrieve all records whose field *field* matches one of the given values

        :param field: the field used for the selection
        :type field: string
        :param values: a list of values to be matched for the given field
        :return: a list of records
        :rtype: list
        """
        self.__check_connection()
        myquery = {
            "query" : {
                    "terms" : { field : values }
                    }
                }
        restot = self.client.search(index=self.database,doc_type=self.collection,body=myquery)['hits']['hits']
        res = [p['_source'] for p in restot]
        if res != []:
            return ( decode_dict(res[i]) for i in range(0,len(res)) )
        return None

#    @profile
    def delete_record(self, record_id):
        """
        Delete an existing record

        :param record_id: record's ID
        """
        self.__check_connection()
        self.logger.debug('deleting document with ID %s', record_id)
        #find in ids
        if isinstance(record_id,dict):
            rid=record_id['_id']+"_"+str(record_id['_version'])
        else:
            rid=record_id
        existing_record=self._get_ids(rid)
        if existing_record:
            er=existing_record['ids']
            f=[elem for elem in er if elem[0]==rid]
            if not f:
                raise MissingRevisionError("A record with ID %s does not exist in ids archive" % record_id)
            found=f[0]
            self.client.delete(index=found[1],doc_type=found[2],id=found[0],refresh="true")
            self._erase_ids(found[0])
        else:
            baseid=rid.rsplit('_', 1)[0]
            existing_record=self._get_ids(baseid)
            if existing_record:
                er=existing_record['ids']
                f=[elem for elem in er if elem[0]==rid]
                if not f:
                    raise MissingRevisionError("A record with ID %s does not exist in ids archive" % record_id)
                found=f[0]
                self.client.delete(index=found[1],doc_type=found[2],id=found[0],refresh="true")
                self._erase_ids(found[0])
            else:
                raise MissingRevisionError("A record with ID %s does not exist in ids archive" % record_id)

    def delete_record2(self, record_id):
        """
        Delete an existing record

        :param record_id: record's ID
        """
        self.__check_connection()
        self.logger.debug('deleting document with ID %s', record_id)
        try:
            if(isinstance(record_id,dict)):
                newid=record_id['_id']+"_"+str(record_id['_version'])
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
            rid = record_id['_id']+"_"+str(record_id['_version'])
        else:
            rid=record_id
        baseid=rid.rsplit('_', 1)[0]
        existing_record=self._get_ids(baseid)
        counter=0
#        self._select_lookup_db(self.database)
        if existing_record:
            er=existing_record['ids']
            new_er=[]
            for elem in er:
                if "_" in elem[0]:
                    if int(elem[0].rsplit('_',1)[1])>version_to_keep:
                        self.client.delete(index=elem[1],doc_type=elem[2],id=elem[0],refresh='true')
                        self._erase_ids(elem[0])
                        counter=counter+1
                    else:
                        new_er.append([elem[0],elem[1],elem[2]])
                else:
                    new_er.append([elem[0],elem[1],elem[2]])
            if new_er:
                existing_record['ids']=new_er
                self.client.index(index=self.database_ids,doc_type=self.doc_ids,id=baseid,
                                        body=existing_record,refresh='true',timeout=self.insert_timeout)
            self.client.indices.refresh(index=self.database_ids)
            return counter
        else:
            return 0

#    @profile
    def delete_later_versions2(self, record_id, version_to_keep=0):
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

#    @profile
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
            #update ids lookup table
            results=forrestrue['hits']['hits']
            for r in results:
                self._erase_ids(r['_id'])
            return restrue
        except elasticsearch.NotFoundError:
            return None


#    @profile
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
                newversion = record_to_update['version']+1
                record_to_update['version']=newversion
                res = self.client.index(index=self.database,doc_type=self.collection_name,body=record_to_update,id=record_id,timeout=self.insert_timeout)
                if(self._is_clinical_record_revision(record_to_update)):
                    self.logger.debug('updated %s document',res[u'_id'].rsplit('_', 1)[0])
                else:
                    self.logger.debug('updated %s document', res[u'_id'])
            else:
                if(self._is_clinical_record_revision(record_to_update)):
                    res = self.client.index(index=self.database,doc_type=self.collection_name,body=record_to_update,id=record_id,timeout=self.insert_timeout)
                    self.logger.debug('updated %s document',res[u'_id'].rsplit('_', 1)[0])
                else:
                    res = self.client.index(index=self.database,doc_type=self.collection_name,body=record_to_update,id=record_id,timeout=self.insert_timeout)
                    self.logger.debug('updated %s document', res[u'_id'])
            return last_update

#    @profile
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
        else:
            newid=record_id
        new_record['_id']=newid
        res = self.client.index(index=self.database,doc_type=self.collection_name,body=new_record,id=record_id,timeout=self.insert_timeout)
        return last_update

#    @profile
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
        res = self.client.index(index=self.database,doc_type=self.collection_name,body=record_to_update,id=record_id,timeout=self.insert_timeout)
        self.logger.debug('updated %s document', res[u'_id'])
        return last_update

#    @profile
    def extend_list(self, record_id, list_label, items, update_timestamp_label,
                    increase_version=False):
        return super(ElasticSearchDriver, self).extend_list(record_id, list_label, items,
                                                            update_timestamp_label, increase_version)

#    @profile
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
        res = self.client.index(index=self.database,doc_type=self.collection_name,body=record_to_update,id=record_id,timeout=self.insert_timeout)
        self.logger.debug('updated %s document', res[u'_id'])
        return last_update

#    @profile
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

#    @profile
    def _parse_expression(self, expression):
        # replace all invalid characters
        for not_allowed_char, allowed_char in self.ENCODINGS_MAP.iteritems():
            expression = expression.replace(not_allowed_char, allowed_char)
        q = expression.replace('/', '.')
        return q

#    @profile
    def _parse_simple_expression(self, expression):
        return super(ElasticSearchDriver, self)._parse_simple_expression(expression)

#    @profile
    def _parse_match_expression(self, expr):
        return super(ElasticSearchDriver, self)._parse_match_expression(expr)

#    @profile
    def _normalize_path(self, path):
        for original_value, encoded_value in self.ENCODINGS_MAP.iteritems():
            path = path.replace(original_value, encoded_value)
        if path.startswith('/'):
            path = path[1:]
        for x, y in [('[', '/'), (']', ''), ('/', '.')]:
            path = path.replace(x, y)
        return path

#    @profile
    def _build_path(self, path):
        path = list(path)
        path[0] = 'ehr_data'
        tmp_path = '.archetype_details.'.join([self._normalize_path(x) for x in path])
        return '%s.archetype_details' % tmp_path

#    @profile
    def _build_paths(self, containment_mapping):
        return super(ElasticSearchDriver, self)._build_paths(containment_mapping)

#    @profile
    def _extract_path_alias(self, path):
        return super(ElasticSearchDriver, self)._extract_path_alias(path)

#    @profile
    def _get_archetype_class_path(self, path):
        path_pieces = path.split('.')
        path_pieces[-1] = 'archetype_class'
        return '.'.join(path_pieces)

#    @profile
    def _calculate_condition_expression(self, condition, variables_map, containment_mapping):
        query = dict()
        paths = self._build_paths(containment_mapping)
        expressions = dict()
        or_indices = list()
        and_indices = list()
        for i, cseq in enumerate(condition.condition.condition_sequence):
            if isinstance(cseq, PredicateExpression):
                left_op_var, left_op_path = self._extract_path_alias(cseq.left_operand)
                expressions[i] = self._map_operand('%s.%s' % (paths[variables_map[left_op_var]],
                                                              self._normalize_path(left_op_path)),
                                                   cseq.right_operand, cseq.operand)
            elif isinstance(cseq, ConditionOperator):
                if cseq.op == 'OR':
                    or_indices.extend([i-1, i+1])
                elif cseq.op == 'AND':
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
            for ai in and_indices:
                if(isinstance(expressions[ai],str)):
                    query.update({str(expressions[ai]) : "$%nothing%$" })
                elif (str(expressions[ai]).find("must_not") == -1 ):
                    query.update({ " \"must\" : "+str(expressions[ai]) : "$%nothing%$" })
                else:
                    query.update(expressions[ai])
        else:
            for e in expressions.values():
                if (str(e).find("must_not") == -1 ):
                    query.update({" \"must\" : "+ str(e) : "$%nothing%$" })
                else:
                    query.update(e)
        return query

#    @profile
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

#    @profile
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

#    @profile
    def _calculate_location_expression(self, location, query_params, patients_collection,
                                       ehr_collection, aliases_mapping):
        query = dict()
        if location.class_expression:
            ce = location.class_expression
            if ce.class_name.upper() == 'EHR':
                query.update(self._calculate_ehr_expression(ce, query_params,
                                                            patients_collection,
                                                            ehr_collection))
                if 'EHR' not in aliases_mapping:
                    aliases_mapping['EHR'] = ce.variable_name
            else:
                if ce.predicate:
                    query.update(self._compute_predicate(ce.predicate))
        else:
            raise MissingLocationExpressionError("Query must have a location expression")
        return query


#    @profile
    def _map_ehr_selection(self, path, ehr_var):
        path = path.replace('%s.' % ehr_var, '')
        if path == 'ehr_id.value':
            return {'patient_id': True}
        if path == 'uid.value':
            return {'_id': True}

#    @profile
    def _calculate_selection_expression(self, selection, variables_map, containment_mapping):
        query = {'_id': False}
        paths = self._build_paths(containment_mapping)
        results_aliases = {}
        for var in selection.variables:
            path = self._normalize_path(var.variable.path.value)
            if var.variable.variable == variables_map['EHR']:
                q = self._map_ehr_selection(path, variables_map['EHR'])
                query.update(q)
                results_aliases[q.keys()[0]] = var.label or '%s%s' % (var.variable.variable,
                                                                      var.variable.path.value)
            else:
                var_path = '%s.%s' % (paths[variables_map[var.variable.variable]], path)
                query[var_path] = True
                results_aliases[var_path] = var.label or '%s%s' % (var.variable.variable,
                                                                   var.variable.path.value)
        return query, results_aliases

#    @profile
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


    def get_records_by_query(self,query,fields=None):
        if self.grbq == "from":
            res=self.get_records_by_query_from(query,fields)
        elif self.grbq == "scan":
            res=self.get_records_by_query_scan(query,fields)
        else:
            print "\nbad grbq:"+self.grbq+" using scan instead"
            res=self.get_records_by_query_scan(query,fields)
        return res

#    @profile
    def get_records_by_query_scan(self, query,fields=None):
        """
        Retrieve all records matching the given query

        :param fields: the fields to be retrieved
        :type  fields: string
        :param query: the value that must be matched for the given field
        :type query: string
        :return: a list of records
        :rtype: list
        """
        size = self.threshold
#        query["size"]=self.threshold
        scrolltime=self.scrolltime
        restot = []
        pippo=False
        sc_id=""
        while not pippo:
            if restot==[]:
                if fields:
                    resu = self.client.search(index=self.database,_source_include=fields,size=size,body=query,scroll=scrolltime)
                else:
                    resu = self.client.search(index=self.database,size=size,body=query,scroll=scrolltime)
                if resu['hits']['hits']==[]:
                    pippo=True
                else:
                    sc_id=resu["_scroll_id"]
                    number_of_results=resu['hits']['total']
                    restot.extend(resu['hits']['hits'])
                    if len(resu['hits']['hits']) < size:
                        pippo=True
            else:
                resuh=self.client.scroll(scroll_id=sc_id, scroll=scrolltime)['hits']
                if resuh['hits']==[]:
                    pippo=True
                else:
                    restot.extend(resuh['hits'])
                    if len(resuh['hits']) < size:
                        pippo=True
        res = [p['_source'] for p in restot]
        if res != []:
            return ( decode_dict(res[i]) for i in range(0,len(res)) )
        return None

    def get_records_by_query_from(self, query,fields=None):
        """
        Retrieve all records matching the given query

        :param fields: the fields to be retrieved
        :type  fields: string
        :param query: the value that must be matched for the given field
        :type query: string
        :return: a list of records
        :rtype: list
        """
        size = self.threshold
        restot = []
        if fields:
            resu = self.client.search(index=self.database,_source_include=fields,size=size,body=query)['hits']
        else:
            resu = self.client.search(index=self.database,size=size,body=query)['hits']
        number_of_results=resu['total']
        restot.extend(resu['hits'])
        if number_of_results > size:
            for i in range(1, (number_of_results-1)/size+1):
                if fields:
                    resu = self.client.search(index=self.database,_source_include=fields,size=size,from_=i*size,body=query)['hits']
                else:
                    resu = self.client.search(index=self.database,size=size,from_=i*size,body=query)['hits']
                restot.extend(resu['hits'])
        res = [p['_source'] for p in restot]
        if res != []:
            return ( decode_dict(res[i]) for i in range(0,len(res)) )
        return None

    def count_records_by_query(self, query):
        """
        Retrieve the count of all records matching the given query
        :param query: the value that must be matched for the given field
        :type query: string
        :return: the count of all matching records
        :rtype: integer
        """
        return self.client.search(index=self.database,body=query,search_type='count')['hits']['total']

#    @profile
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
        selected_fields=self._collate_selected_fields(fields)
#        query_results = self.get_records_by_query(query)
        query_results = self.get_records_by_query(query,selected_fields)
        if close_conn_after_done:
            self.disconnect()
        else:
            self.select_collection(original_collection)
        if query_results:
            for q in query_results:
                record = dict()
                for x in self._split_results(q):
                    record[x[0]] = x[1]
                rr = ResultRow(record)
                rs.add_row(rr)
        return rs

    def _run_aql_count(self, query, collection):
        self.logger.debug("Running count query\n%s\nwith filters\n%s", query)
        if self.is_connected:
            original_collection = self.collection
            close_conn_after_done = False
        else:
            close_conn_after_done = True
        self.connect()
        self.select_collection(collection)
        qcount = self.count_records_by_query(query)
        if close_conn_after_done:
            self.disconnect()
        else:
            self.select_collection(original_collection)
        return qcount

    def _collate_selected_fields(self,sfields):
        lfields=[]
        for sf in sfields:
            if sfields[sf] == True:
                lfields.append(sf)
        return ",".join(lfields)

#    @profile
    def apply_selection(self,record,fields):
        q={}
        for f in fields:
            if fields[f] == True:
                if f in record:
                    q.update({f : record[f]})
        return q

#    @profile
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
        return super(ElasticSearchDriver, self).build_queries(query_model, patients_repository, ehr_repository,
                                                      query_params)

    def _get_query_hash(self, query):
        return super(ElasticSearchDriver, self)._get_query_hash(query)

    def _get_queries_hash_map(self, queries):
        return super(ElasticSearchDriver, self)._get_queries_hash_map(queries)

    def _get_structures_hash_map(self, queries):
        return super(ElasticSearchDriver, self)._get_structures_hash_map(queries)

    def _get_structures_selector(self, structure_ids):
        if len(structure_ids) == 1:
            return {"\"must\" : {\"term\" : {\"ehr_structure_id\" : \""+str(structure_ids[0])+"\"} }": "$%nothing%$"}
        else:
            return {"\"must\" : {\"terms\" : {\"ehr_structure_id\" : "+str([structure_ids])+",\"execution\" : \"or\" } }" : "$%nothing%$"}

    def _aggregate_queries(self, queries):
        aggregated_queries = list()
        queries_hash_map = self._get_queries_hash_map(queries)
        structures_hash_map = self._get_structures_hash_map(queries)
        for qhash, structures in structures_hash_map.iteritems():
            ql=[]
            query = queries_hash_map[qhash]
            q1=self._clean_piece(query['condition'])
            ql.append(" \"query\" : { \"bool\" : "+str(q1)+"}")
            q2=self._clean_piece(self._get_structures_selector(structures))
            ql.append(" \"filter\" : { \"bool\" : "+str(q2)+"}")
            qs=",".join(ql)
            qtot=" \"query\" : { \"filtered\" : {"+qs + "}}"
            qtot={ qtot : "$%nothing%$"}
#            query['condition'].update(qtot)
            query['condition']=qtot
#            query['condition'].update(self._get_structures_selector(structures))
            aggregated_queries.append(query)
        return aggregated_queries

#    @profile
    def execute_query(self, query_model, patients_repository, ehr_repository,
                      query_params=None, count_only=False, query_processes=1):
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
        queries = self.build_queries(query_model, patients_repository, ehr_repository,
                                     query_params)
        aggregated_queries = self._aggregate_queries(queries)
        total_queries=[]
        #finalize queries creation
        for query in aggregated_queries:
            single_query={}
            query_string=self._clean_piece(query['condition'])
            query_string=self._final_check(query_string)
            single_query.update({'condition':query_string})
            single_query.update({'selection':query['selection']})
            single_query.update({'aliases':query['aliases']})
            total_queries.append(single_query)
        if count_only:
            return self._count_only_queries(total_queries,ehr_repository)
        else:
            return self._regular_queries(total_queries,ehr_repository,query_processes)

    def _regular_queries(self,total_queries,ehr_repository,query_processes):

        total_results = ResultSet()
        if query_processes == 1 or len(total_queries) == 1:
            for i in range(0,len(total_queries)):
                results = self._run_aql_query(total_queries[i]['condition'], fields=total_queries[i]['selection'],
                                          aliases=total_queries[i]['aliases'], collection=ehr_repository)
                total_results.extend(results)
        else:
            queries_pool = Pool(query_processes)
            results = queries_pool.imap_unordered( MultiprocessQueryRunner(self.host, self.database,
                                                    ehr_repository, self.port, self.user,self.passwd),total_queries)
            for r in results:
                total_results.extend(r)
        return total_results

    def _count_only_queries(self,total_queries,ehr_repository):

        count=0
        for i in range(0,len(total_queries)):
            cresult = self._run_aql_count(total_queries[i]['condition'], collection=ehr_repository)
            count=count+cresult
        return count
#    @profile
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



#    @profile
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
                        print "bad piece to cleanup: %s" % newp
                        raise QueryCreationException()
                    else:
                        positionc=len(newpr)-positionc-1
                        poscolon.append(positionc)
                    #find position of first right parenthesis
                    position=newp.find("}",positionn+10,len(newp))
                    position2=newp.find("]",positionn+10,len(newp))
                    isbrace=True
                    if( position==-1 and position2==-1):
                        print "not found parenthesis for a given nothing :D"
                        print "bad piece to cleanup: %s" % newp
                        raise QueryCreationException()
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
#    @profile
    def get_selection_hash(self,selection):
        sel_hash = md5()
        sel_hash.update(json.dumps(selection))
        return sel_hash.hexdigest()
