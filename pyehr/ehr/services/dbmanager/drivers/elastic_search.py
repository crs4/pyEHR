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
            '_id': clinical_record_revision.record_id,
            'ehr_structure_id': clinical_record_revision.structure_id,
            'patient_id': clinical_record_revision.patient_id,
            'creation_time': clinical_record_revision.creation_time,
            'last_update': clinical_record_revision.last_update,
            'active': clinical_record_revision.active,
            'ehr_data': ehr_data,
            '_version': clinical_record_revision.version
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
            record_id=record['_id'],
            structure_id=record['ehr_structure_id'],
            version=record.get('_version'),
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
        if 'ehr_data' in record:
            if isinstance(record.get('_id'), dict):
                return self._decode_clinical_record_revision(record)
            else:
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
            res = self.client.get_source(index=self.database,id=record_id,version=version)
            return decode_dict(res)
        except elasticsearch.NotFoundError:
            return None

    def get_revisions_by_ehr_id(self, record_id):
        """
        Retrieve all revisions for the given EHR ID

        :param ehr_id: the EHR ID that will be used to retrieve revisions
        :return: all revisions matching given ID
        :rtype: list
        """
        return self.get_records_by_query({'_id._id': ehr_id})


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
            "filter" : {
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
#            return {"{\"term\" : "+str({left : right})+"}" : pippo}
#            return "{\"term\" : "+str({left : right})+"}"
            return {"{\"match\" : "+str({left : right})+"}" : "nothing"}
        elif operand == "!=":
#            return  { "{\"filter\" : { \"not\" : { \"term\" : "+str({left : right})+"}}}" :
#                          "pippo"}
#            return   "{\"filter\" : { \"not\" : { \"term\" : "+str({left : right})+"}}}"
            return   { " \"filter\" : { \"not\" : { \"match\" : "+str({left : right})+"}} : " : "nothing" }
        elif operand in operands_map:
#            return {"{\"range\" :  "+str({left: {operands_map[operand]: right}})+"}" :
#            "pippo" }
#            return "{\"range\" :  "+str({left: {operands_map[operand]: right}})+"}"
            return { "{ \"range\" :  "+str({left: {operands_map[operand]: right}})+"}" : "nothing" }
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
                    query.update({"\"must\" :{ \"match\" : "+str({self._get_archetype_class_path(p[k]): aliases[k]['archetype_class']})+"}" : "nothing" })
#                    query.update({self._get_archetype_class_path(p[k]): aliases[k]['archetype_class']})
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
 #                   or_statement = {'$or': [expressions[i] for i in or_indices]}
 #                   or_statement= {"{\"bool\" : { \"should\" : "+str([ expressions[i].keys() for i in or_indices])+",\"minimum_should_match\" : 1}}" : "nothing" }
                    or_statement= " \"should\" : "+str([ expressions[i].keys() for i in or_indices])+",\"minimum_should_match\" : 1"
#                    or_statement=or_statement.replace("[[","[")
#                    or_statement=or_statement.replace("]]","]")
                    expressions[max(expressions.keys()) + 1] = or_statement
                    and_indices.append(max(expressions.keys()))
#                    query.update({or_statement : "nothing" })
                if len(and_indices) > 0:
                    for i in and_indices:
#                        print "oooooooooooooooooo"
#                        print expressions[i]
                        query.update({str(expressions[i]) : "nothing" })
                else:
                    for e in expressions.values():
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
                        query.update({"{ \"match\" : {"+ str({lo : ro})+"}}" : "nothing" })
#                        query[lo] = ro
            else:
                raise PredicateException("No predicate expression found")
        elif type(predicate) == ArchetypePredicate:
            predicate_string = predicate.archetype_id
            query.update({"{ \"filter\" : {\"exists\" : { \"field\" : "+ str(predicate_string)+"}}}" : "nothing"} )
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
                    query.update({ " \"must\" : { \"term\" : {\"patient_id\": \"" + str(right_operand).lower() +"\"}}" : "nothing"})
                elif pr.left_operand == 'id':
                    # use given EHR ID
                    query.update(self._map_operand(pr.left_operand,
                                                   right_operand,
                                                   pr.operand))
                else:
                    query.update(self._compute_predicate(pr))
            else:
                raise PredicateException('No left operand in predicate')
            # print "in ehr_expression"
            # print query
            # print "in ehr_expression end"
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
#        query.update({"{\"terms\" : {\"ehr_structure_id\" : "+str([structure_ids])+",\"minimum_should_match\" : 1 } }" :
#                          {'ehr_structure_id': {'$in': structure_ids}}})
        query.update({"\"must\" : {\"terms\" : {\"ehr_structure_id\" : "+str([structure_ids])+",\"execution\" : \"or\" } }" : "nothing"})
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
                # print "in calculate selection"
                # print q
                # print "in calc---"
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
        resu = self.client.search(index=self.database,doc_type=self.collection,size=size,body=query)['hits']
        number_of_results=resu['total']
        restot.extend(resu['hits'])
        if number_of_results > size:
            for i in range(1, (number_of_results-1)/size+1):
                resu = self.client.search(index=self.database,doc_type=self.collection,size=size,from_=i*size,body=query)['hits']
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
        # print '----risultati--------'
        # print query_results
        if close_conn_after_done:
            self.disconnect()
        else:
            self.select_collection(original_collection)
        if query_results:
            for q in query_results:
#                print "--------inside loop------"
#               print q
                record = dict()
                for x in self._split_results(q):
                    record[x[0]] = x[1]
#               print record
#               record_sel_al = self.apply_selection_and_aliases(record,fields,aliases)
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
        # print "\nlocation query\n"
        # print location_query
        # print "\naliases:\n"
        # print aliases
        # print "\nehr_alias\n"
        # print ehr_alias
        # print "\ndopo--------\n"
        if not location_query:
            return queries, []
        if condition:
            condition_results = self._calculate_condition_expression(condition, aliases)
            # print "\ncondition_results\n"
            # print condition_results
            # print "\ndopo-----cr\n"
            for condition_query, mappings in condition_results:
                # print "condquery,result\n"
                # print condition_query
                # print "\n---\n"
                # print mappings
                # print "\n-----\n"
                # for condq in condition_query:
                #     print "\ncondq--->"
                #     print condq
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
                # print "------------A----------"
                # print a
                for p in [dict(izip(paths.keys(), a))]:
                    # print "---------P-----------"
                    # print p
                    q = dict()
                    for k in aliases.keys():
                        q.update({"\"must\" : { \"match\" : "+str({self._get_archetype_class_path(p[k]): aliases[k]['archetype_class']})+"}" : "nothing" })
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
        # print "\nlocation_query\n"
        # print location_query
        # print '\nqueries\n'
        # print queries
        for x in queries:
            # print "----query----"
            # print x
            if x not in query_mappings:
                query_mappings.append(x)
        # print "--------------------"
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
        # print 'selection_mappings'
        # print selection_mappings
        # print 'filter mappings'
        # print filter_mappings

        querylist=[]
        for sk, sm in selection_mappings.iteritems():
#            q = {'$or': filter_mappings[sk]}
#             print "+++++++++"
#             print filter_mappings[sk]
#             print "+++++++++++++"
            querylist.append("{ \"query\" : { \"bool\" : "+str(filter_mappings[sk][0].keys())+"}}")
            querylist.append("\"filter\" : { \"bool\" : "+str(location_query.keys())+"}}}")
            # print "querylist----------"
            # print querylist
            # print "ql---------------"
            ql=self.cleanquery(querylist)
#            print "\n-------------AAAA----------------------"
#            print ql
#            print "---------------BBBB--------------------"
            results = self._run_aql_query(ql, sm['selection_filter'], aliases=sm['aliases'],
                                          collection=ehr_repository)
            total_results.extend(results)
            querylist[:] = []
        return total_results
    def get_selection_hash(self,selection):
        sel_hash = md5()
        sel_hash.update(json.dumps(selection))
        return sel_hash.hexdigest()
    def cleanquery(self,querylist):
#        newlist=[]
#        for i in querylist:
#            for j in i:
#            newlist.append(j)
#        print "newlist"
#        print newlist
#        print "------"
        q1 = ",".join(querylist)
        q1 = q1.replace("} }']","} } }").replace("[\'","{").replace("\\\'","\"").replace("\\\\\"","\"").replace("\'\"","\"")\
            .replace("\'","\"").replace("\" \"","\"").replace("}\"]","}]").replace("[\"{","[{")\
            .replace("}\",","},").replace("[[","@").replace("]]","%").replace("[","").replace("]","")\
            .replace("@","[").replace("%","]").replace("{{\"terms","{\"terms").replace(": 1\\\"}",": 1}").replace(": 1\"}}",": 1}}")
        qtot="{ \"query\" : { \"filtered\" : " + q1+ "}}"
        return qtot