from pyehr.aql.parser import *
from pyehr.ehr.services.dbmanager.drivers.interface import DriverInterface
from pyehr.ehr.services.dbmanager.querymanager.results_wrappers import *
from pyehr.ehr.services.dbmanager.errors import *
from pyehr.utils import *
import elasticsearch
import time
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
        #self.collection_name = collection
        # self.port = port
        # self.user = user
        # self.passwd = passwd
        self.index_service = index_service
        self.logger = logger or get_logger('elasticsearch-db-driver')

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

    def _encode_clinical_record(self, clinical_record):
        def normalize_keys(document, original, encoded):
            normalized_doc = {}
            for k, v in document.iteritems():
                k = k.replace(original, encoded)
                if not isinstance(v, dict):
                    normalized_doc[k] = v
                else:
                    normalized_doc[k] = normalize_keys(v, original, encoded)
            return normalized_doc

        ehr_data = clinical_record.ehr_data.to_json()
        if self.index_service:
            structure_id = self.index_service.get_structure_id(ehr_data)
        else:
            structure_id = None
        for original_value, encoded_value in self.ENCODINGS_MAP.iteritems():
            ehr_data = normalize_keys(ehr_data, original_value, encoded_value)
        encoded_record = {
            'patient_id': clinical_record.patient_id,
            'creation_time': clinical_record.creation_time,
            'last_update': clinical_record.last_update,
            'active': clinical_record.active,
            'ehr_data': ehr_data,
            '_id' : clinical_record.record_id
        }
        if structure_id:
            encoded_record['ehr_structure_id'] = structure_id
        return encoded_record

    def encode_record(self, record):
        """
        Encode a :class:`Record` object into a data structure that can be saved within
        ElasticSearch

        :param record: the record that must be encoded
        :type record: a :class:`Record` subclass
        :return: the record encoded as a ElasticSearch document
        """
        from pyehr.ehr.services.dbmanager.dbservices.wrappers import PatientRecord, ClinicalRecord

        if isinstance(record, PatientRecord):
            return self._encode_patient_record(record)
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
                record_id=str(record.get('_id')),
            )
        else:
            return PatientRecord(
                creation_time=record['creation_time'],
                record_id=str(record.get('_id'))
            )

    def _decode_clinical_record(self, record, loaded):
        from pyehr.ehr.services.dbmanager.dbservices.wrappers import ClinicalRecord,\
            ArchetypeInstance

        def decode_keys(document, encoded, original):
            normalized_doc = {}
            for k, v in document.iteritems():
                k = k.replace(encoded, original)
                if not isinstance(v, dict):
                    normalized_doc[k] = v
                else:
                    normalized_doc[k] = decode_keys(v, encoded, original)
            return normalized_doc

        record = decode_dict(record)
        if loaded:
            ehr_data = record['ehr_data']
            for original_value, encoded_value in self.ENCODINGS_MAP.iteritems():
                ehr_data = decode_keys(ehr_data, encoded_value, original_value)
            crec = ClinicalRecord(
                ehr_data=ArchetypeInstance.from_json(ehr_data),
                creation_time=record['creation_time'],
                last_update=record['last_update'],
                active=record['active'],
                record_id=str(record.get('_id'))
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
                record_id=str(record.get('_id')),
                last_update=record.get('last_update'),
                active=record.get('active'),
                ehr_data=arch
            )
            if 'patient_id' in record:
                crec._set_patient_id(record['patient_id'])
            return crec

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
        if 'ehr_data' in record:
            return self._decode_clinical_record(record, loaded)
        else:
            return self._decode_patient_record(record, loaded)

    def count(self):
        return self.client.count(index=self.database,doc_type=self.collection)['count']

    def count2(self):
        return self.client.search(index=self.database,doc_type=self.collection)['hits']['total']

    def count3(self):
        return self.client.search(index=self.database)['hits']['total']

    def add_record(self, record):
        """
        Save a record within ElasticSearch and return the record's ID

        :param record: the record that is going to be saved
        :type record: dictionary
        :return: the ID of the record
        """
        self.__check_connection()
        try:
            if(record.has_key('_id')):
                return str(self.client.index(index=self.database,doc_type=self.collection,id=record['_id'],body=record,op_type='create',refresh='true')['_id'])
            else:
                return str(self.client.index(index=self.database,doc_type=self.collection,body=record,op_type='create',refresh='true')['_id'])
        except elasticsearch.ConflictError:
            raise DuplicatedKeyError('A record with ID %s already exists' % record['_id'])


    def pack_record(self,records):
        first="{\"create\":{\"_index\":\""+self.database+"\",\"_type\":\""+self.collection+"\""
        puzzle=""
        for dox in records:
            puzzle=puzzle+first
            if(dox.has_key("_id")):
                puzzle = puzzle+",\"_id\":\""+dox["_id"]+"\"}}\n{"
            else:
                puzzle=puzzle+"}}\n{"
            for k in dox:
                puzzle=puzzle+"\""+k+"\":\""+str(dox[k])+"\","
            puzzle=puzzle.strip(",")+"}\n"
        return puzzle

    # def add_records(self, records):
    #     """
    #     Save a list of records within ElasticSearch and return records' IDs
    #
    #     :param records: the list of records that is going to be saved
    #     :type record: list
    #     :return: a list of records' IDs
    #     :rtype: list
    #     """
    #     self.__check_connection()
    #     #create a bulk list
    #     bulklist = self.pack_record(records)
    #     bulkanswer = self.client.bulk(body=bulklist,index=self.database,doc_type=self.collection,refresh='true')
    #     if(bulkanswer['errors']): # there are errors
    #         #count the errors
    #         nerrors=0
    #         err=[]
    #         errtype=[]
    #         for b in bulkanswer['items']:
    #             if(b['create'].has_key('error')):
    #                 err[nerrors] = b['create']['_id']
    #                 errtype[nerrors] = b['create']['error']
    #                 nerrors += 1
    #         if(nerrors):
    #             raise DuplicatedKeyError('Record with these id already exist: %s' %err)
    #         else:
    #             print 'bad programmer'
    #             sys.exit(1)
    #     else:
    #         return [str(g['create']['_id']) for g in bulkanswer['items']]

    def add_records(self, records, skip_existing_duplicated=False):
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
            res = self.client.get_source(index=self.database,id=str(record_id))
            return decode_dict(res)
        except elasticsearch.NotFoundError:
            return None

    def get_record_by_version(self, record_id, version):
        raise NotImplementedError()

    def get_all_records(self):
        """
        Retrieve all records within current collection

        :return: all the records stored in the current collection
        :rtype: list
        """
        self.__check_connection()
        restot = self.client.search(index=self.database,doc_type=self.collection)['hits']['hits']
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
            "query" : {
                    "term" : { field : value }
                    }
                }
        restot = self.client.search(index=self.database,doc_type=self.collection,body=myquery)['hits']['hits']
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
            res=self.client.delete(index=self.database,doc_type=self.collection,id=record_id,refresh='true')
            return res
        except elasticsearch.NotFoundError:
            return None

    def delete_later_versions(self, record_id, version_to_keep=0):
        raise NotImplementedError()

    def delete_records_by_query(self, query):
        raise NotImplementedError()

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
        :return: the timestamp of the last update as saved in the DB or None (if update_timestamp_field was None)
        """
        record_to_update = self.get_record_by_id(record_id)
        if record_to_update is None:
            self.logger.debug('No record found with ID %r', record_id)
            return None
        else:
            record_to_update[field_label]= field_value
            if update_timestamp_label:
                last_update = time.time()
                record_to_update['last_update']=last_update
            else:
                last_update=None
            res = self.client.index(index=self.database,doc_type=self.collection,body=record_to_update,id=record_id)
            self.logger.debug('updated %s document', res[u'_id'])
            return last_update

    def replace_record(self, record_id, new_record, update_timestamp_label=None):
        raise NotImplementedError

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
        res = self.client.index(index=self.database,doc_type=self.collection,body=record_to_update,id=record_id)
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
        res = self.client.index(index=self.database,doc_type=self.collection,body=record_to_update,id=record_id)
        self.logger.debug('updated %s document', res[u'_id'])
        return last_update

    def _map_operand(self, left, right, operand):
        raise NotImplementedError()

    def _parse_expression(self, expression):
        raise NotImplementedError()

    def _parse_simple_expression(self, expression):
        super(ElasticSearchDriver, self)._parse_expression(expression)

    def _parse_match_expression(self, expr):
        super(ElasticSearchDriver, self)._parse_match_expression(expr)

    def _normalize_path(self, path):
        raise NotImplementedError()

    def _build_path(self, path):
        raise NotImplementedError()

    def _build_paths(self, aliases):
        super(ElasticSearchDriver, self)._build_paths(aliases)

    def _extract_path_alias(self, path):
        super(ElasticSearchDriver, self)._extract_path_alias(path)

    def _get_archetype_class_path(self, path):
        super(ElasticSearchDriver, self)._get_archetype_class_path(path)

    def _calculate_condition_expression(self, condition, aliases):
        raise NotImplementedError()

    def _compute_predicate(self, predicate):
        raise NotImplementedError()

    def _calculate_ehr_expression(self, ehr_class_expression, query_params, patients_collection,
                                  ehr_collection):
        raise NotImplementedError()

    def _calculate_location_expression(self, location, query_params, patients_collection,
                                       ehr_collection):
        raise NotImplementedError()

    def _calculate_selection_expression(self, selection, aliases):
        raise NotImplementedError()

    def _calculate_selection_expression(self, selection, aliases, ehr_alias):
        pass

    def _split_results(self, query_results):
        raise NotImplementedError()

    def _run_aql_query(self, query, fileds, aliases, collection):
        raise NotImplementedError()

    def build_queries(self, query_model, patients_repository, ehr_repository, query_params=None):
        super(ElasticSearchDriver, self).build_queries(query_model, patients_repository,
                                                       ehr_repository, query_params)

    def execute_query(self, query_model, patients_repository, ehr_repository,
                      query_parameters):
        raise NotImplementedError()