from pyehr.aql.parser import *
from pyehr.ehr.services.dbmanager.drivers.interface import DriverInterface
from pyehr.ehr.services.dbmanager.querymanager.results_wrappers import ResultSet,\
    ResultColumnDef, ResultRow
from pyehr.ehr.services.dbmanager.errors import *
from pyehr.utils import *
import pymongo
import pymongo.errors
import time
from hashlib import md5
from multiprocessing import Pool

try:
    import simplejson as json
except ImportError:
    import json


class MultiprocessQueryRunnerPM2(object):

    def __init__(self, host, database, collection,
                 port, user, passwd):
        self.host = host
        self.database = database
        self.collection_name = collection
        self.port = port
        self.user = user
        self.passwd = passwd

    def __call__(self, query_description):
        driver_instance = MongoDriverPM2(
            self.host, self.database, self.collection_name,
            self.port, self.user, self.passwd
        )
        results = driver_instance._run_aql_query(
            query_description['condition'], query_description['selection'],
            query_description['aliases'], self.collection_name
        )
        return results


class MongoDriverPM2(DriverInterface):
    """
    Create a driver to handle I\O with a MongoDB server. Using the given *host* and, if needed, *port*, *user*
    and *password* the driver will contact MongoDB when a connection is needed and will interrogate a specific
    *collection* stored in one *database* within the server. If no *logger* object is passed to constructor, a
    new one is created.
    """

    # This map is used to encode\decode data when writing\reading to\from MongoDB
    ENCODINGS_MAP = {'.': '-'}

    def __init__(self, host, database, collection,
                 port=None, user=None, passwd=None,
                 index_service=None, logger=None):
        self.client = None
        self.database = None
        self.collection = None
        self.host = host
        self.database_name = database
        self.collection_name = collection
        self.port = port
        self.user = user
        self.passwd = passwd
        self.index_service = index_service
        self.logger = logger or get_logger('mongo-db-driver')

    def connect(self):
        """
        Open a connection to a MongoDB server.
        """
        if not self.client:
            self.logger.debug('connecting to host %s', self.host)
            try:
                self.client = pymongo.MongoClient(self.host, self.port)
            except pymongo.errors.ConnectionFailure:
                raise DBManagerNotConnectedError('Unable to connect to MongoDB at %s:%s' %
                                                 (self.host, self.port))
            self.logger.debug('binding to database %s', self.database_name)
            self.database = self.client[self.database_name]
            if self.user:
                self.logger.debug('authenticating with username %s', self.user)
                self.database.authenticate(self.user, self.passwd)
            self.logger.debug('using collection %s', self.collection_name)
            self.collection = self.database[self.collection_name]
        else:
            self.logger.debug('Already connected to database %s, using collection %s',
                              self.database_name, self.collection_name)

    def disconnect(self):
        """
        Close a connection to a MongoDB server.
        """
        self.logger.debug('disconnecting from host %s', self.client.host)
        self.client.disconnect()
        self.database = None
        self.collection = None
        self.client = None

    def init_structure(self, structure_def):
        # MongoDB doesn't need structures initialization
        pass

    @property
    def is_connected(self):
        """
        Check if the connection to the MongoDB server is opened.

        :rtype: boolean
        :return: True if the connection is open, False if it is closed.
        """
        return self.client is not None

    def _check_connection(self):
        if not self.is_connected:
            raise DBManagerNotConnectedError('Connection to host %s is closed' % self.host)

    def select_collection(self, collection_label):
        """
        Change the collection for the current database

        :param collection_label: the label of the collection that must be selected
        :type collection_label: string
        """
        self._check_connection()
        self.logger.debug('Changing collection for database %s, old collection: %s - new collection %s',
                          self.database.name, self.collection.name, collection_label)
        self.collection = self.database[collection_label]

    def _encode_patient_record(self, patient_record):
        encoded_record = {
            'creation_time': patient_record.creation_time,
            'last_update': patient_record.last_update,
            'active': patient_record.active,
            'ehr_records': [ehr.record_id for ehr in patient_record.ehr_records]
        }
        if patient_record.record_id:
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
        MongoDB

        :param record: the record that must be encoded
        :type record: a :class:`Record` subclass
        :return: the record encoded as a MongoDB document
        """
        from pyehr.ehr.services.dbmanager.dbservices.wrappers import PatientRecord,\
            ClinicalRecord, ClinicalRecordRevision

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
                ehr_data = self._decode_keys(ehr_data, encoded_value, original_value)
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
            crec = ClinicalRecord(
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
        Create a :class:`Record` object from data retrieved from MongoDB

        :param record: the MongoDB record that must be decoded
        :type record: a MongoDB dictionary
        :param loaded: if True, return a :class:`Record` with all values, if False all fields with
          the exception of the record_id one will have a None value
        :type loaded: boolean
        :return: the MongoDB document encoded as a :class:`Record` object
        """
        if 'ehr_data' in record:
            if isinstance(record.get('_id'), dict):
                return self._decode_clinical_record_revision(record)
            else:
                return self._decode_clinical_record(record, loaded)
        else:
            return self._decode_patient_record(record, loaded)

    def add_record(self, record):
        """
        Save a record within MongoDB and return the record's ID

        :param record: the record that is going to be saved
        :type record: dictionary
        :return: the ID of the record
        """
        self._check_connection()
        try:
            return self.collection.insert(record)
        except pymongo.errors.DuplicateKeyError:
            raise DuplicatedKeyError('A record with ID %s already exists' % record['_id'])

    def add_records(self, records, skip_existing_duplicated=False):
        """
        Save a list of records within MongoDB and return records' IDs. If skip_existing_duplicated is
        True, ignore duplicated key errors and continue with the save process, if it is False a
        DuplicatedKeyError will be raised and no record will be saved.

        :param records: the list of records that is going to be saved
        :type records: list
        :param skip_existing_duplicated: ignore duplicated key errors and save records with a unique ID
        :type skip_existing_duplicated: bool
        :return: a list of records' IDs and a list of records that caused a duplicated key error
        """
        # check for duplicated ID in records' batch
        self._check_batch(records, '_id')
        self._check_connection()
        records_map = {r['_id']: r for r in records}
        duplicated_ids = [x['_id'] for x in self.get_records_by_query({'_id': {'$in': records_map.keys()}},
                                                                      {'_id': True})]
        if len(duplicated_ids) > 0 and not skip_existing_duplicated:
            raise DuplicatedKeyError('The following IDs are already in use: %s' % duplicated_ids)
        try:
            return self.collection.insert([x for k, x in records_map.iteritems() if k not in duplicated_ids]),\
                [records_map[x] for x in duplicated_ids]
        except pymongo.errors.InvalidOperation:
            # empty bulk insert
            return [], [records_map[x] for x in duplicated_ids]

    def get_record_by_id(self, record_id):
        """
        Retrieve a record using its ID

        :param record_id: the ID of the record
        :return: the record or None if no match was found
        :rtype: dict or None
        """
        self._check_connection()
        res = self.collection.find_one({'_id': record_id})
        if res:
            return decode_dict(res)
        else:
            return res

    def get_record_by_version(self, record_id, version):
        """
        Retrieve a record using its ID and version number

        :param record_id: the ID of the record
        :param version: the version number of the record
        :type version: int
        :return: the record or None if no match was found
        :rtype: dict or None
        """
        return self.get_record_by_id({'_id': record_id, '_version': version})

    def get_revisions_by_ehr_id(self, ehr_id):
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
        self._check_connection()
        return (decode_dict(rec) for rec in self.collection.find())

    def get_records_by_value(self, field, value):
        """
        Retrieve all records whose field *field* matches the given value

        :param field: the field used for the selection
        :type field: string
        :param value: the value that must be matched for the given field
        :return: a list of records
        :rtype: list
        """
        return self.get_records_by_query({field: value})

    def get_records_by_query(self, selector, fields=None, limit=0):
        """
        Retrieve all records matching the given query

        :param selector: the selector (in MongoDB syntax) used to select data
        :type selector: dictionary
        :param fields: a list of field names that should be returned in the result set or a dict specifying the
                       fields to include or exclude
        :type fields: list or dictionary
        :param limit: the maximum number of records that will be fetched by the query, default value is 0
                      which means that limit won't be applied and all records will be fetched
        :type limit: int
        :return: a list with the matching records
        :rtype: list
        """
        self._check_connection()
        return (decode_dict(rec) for rec in self.collection.find(selector, fields, limit=limit))

    def get_values_by_record_id(self, record_id, values_list):
        """
        Retrieve values in *values_list* from record with ID *record_id*

        :param record_id: the ID of the record that must be selected
        :type record_id: str
        :param values_list: a list of field labels that must be extracted from the record
        :type values_list: list
        :return: a dictionary with *values_list* elements as keys
        :rtype: dict
        """
        self._check_connection()
        selector = dict([(value, 1) for value in values_list])
        if '_id' not in values_list:
            selector['_id'] = 0
        return self.collection.find_one(record_id, selector)

    def count_records_by_query(self, selector):
        """
        Retrieve the number of records matching the given query

        :param selector: the selector (in MongoDB syntax) used to select data
        :return: the number of records that match the given query
        :rtype: int
        """
        self._check_connection()
        res = self.collection.find(selector)
        return res.count()

    def delete_record(self, record_id):
        """
        Delete an existing record

        :param record_id: record's ID
        """
        self._check_connection()
        self.logger.debug('deleting document with ID %s', record_id)
        res = self.collection.remove(record_id)
        self.logger.debug('deleted %d documents', res[u'n'])

    def delete_records_by_id(self, records_id):
        self.logger.debug('deleting documents %r' % records_id)
        return self.delete_records_by_query({'_id': {'$in': records_id}})

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
        return self.delete_records_by_query({'_id._id': record_id,
                                             '_version': {'$gt': version_to_keep}})

    def delete_records_by_query(self, query):
        """
        Delete all records that match the given query

        :param query: the query used to select records that will be deleted
        :type query: dict
        :return: the number of deleted records
        :rtype: int
        """
        self._check_connection()
        res = self.collection.remove(query)
        self.logger.debug('Deleted %d documents', res[u'n'])
        return res[u'n']

    def _update_record(self, record_id, update_condition):
        """
        Update an existing record

        :param record_id: record's ID
        :param update_condition: the update condition (in MongoDB syntax)
        :type update_condition: dictionary
        """
        self._check_connection()
        self.logger.debug('Updating record with ID %r, with condition %r', record_id,
                          update_condition)
        res = self.collection.update({'_id': record_id}, update_condition)
        self.logger.debug('updated %d documents', res[u'n'])

    @property
    def documents_count(self):
        """
        Get the number of documents within current collection

        :return: the number of current collection's documents
        :rtype: int
        """
        self._check_connection()
        return self.collection.count()

    def _update_record_timestamp(self, timestamp_field, update_statement):
        last_update = time.time()
        update_statement.setdefault('$set', {})[timestamp_field] = last_update
        self.logger.debug('Update statement is %r', update_statement)
        return update_statement, last_update

    def _increase_version(self, update_statement):
        update_statement.setdefault('$inc', {})['_version'] = 1
        return update_statement

    def update_field(self, record_id, field_label, field_value, update_timestamp_label=None,
                     increase_version=False):
        """
        Update record's field *field* with given value

        :param record_id: record's ID
        :param field_label: field's label
        :type field_label: string
        :param field_value: new value for the selected field
        :param update_timestamp_label: the label of the *last_update* field of the record
                                       if the last update timestamp must be recorded or None
        :type update_timestamp_label: field label or None
        :param increase_version: if True, increase record's version number by 1
        :type increase_version: bool
        :return: the timestamp of the last update as saved in the DB or None
                 (if *update_timestamp_label* was None)
        """
        update_statement = {'$set': {field_label: field_value}}
        if update_timestamp_label:
            update_statement, last_update = self._update_record_timestamp(update_timestamp_label,
                                                                          update_statement)
        else:
            last_update = None
        if increase_version:
            update_statement = self._increase_version(update_statement)
        self._update_record(record_id, update_statement)
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
        self._check_connection()
        last_update = None
        if update_timestamp_label:
            last_update = time.time()
            new_record[update_timestamp_label] = last_update
        try:
            new_record.pop('_id')
        except KeyError:
            pass
        self._update_record(record_id, new_record)
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
        :type update_timestamp_label: string or None
        :return: the timestamp of the last update as saved in the DB or None (if update_timestamp_field was None)
        """
        update_statement = {'$addToSet': {list_label: item_value}}
        if update_timestamp_label:
            update_statement, last_update = self._update_record_timestamp(update_timestamp_label,
                                                                          update_statement)
        else:
            last_update = None
        if increase_version:
            update_statement = self._increase_version(update_statement)
        self._update_record(record_id, update_statement)
        return last_update

    def extend_list(self, record_id, list_label, items, update_timestamp_label=None,
                    increase_version=False):
        """
        Extend a document's list with the list provided

        :param record_id: record's ID
        :param list_label: the label of the field containing the list
        :type list_label: string
        :param items: the items that will be appended to the list
        :param update_timestamp_label:the label of the *last_update* field of the record if the last update timestamp
          must be recorded or None
        :type update_timestamp_label: string or None
        :return: the timestamp of the last update as saved in the DB or None (if update_timestamp_field was None)
        """
        update_statement = {'$addToSet': {list_label: {'$each': items}}}
        if update_timestamp_label:
            update_statement, last_update = self._update_record_timestamp(update_timestamp_label,
                                                                          update_statement)
        else:
            last_update = None
        if increase_version:
            update_statement = self._increase_version(update_statement)
        self._update_record(record_id, update_statement)
        return last_update

    def remove_from_list(self, record_id, list_label, item_value, update_timestamp_label=None,
                         increase_version=False):
        """
        Remove a value from a list within a document

        :param record_id: record's ID
        :param list_label: the label of the field containing the list
        :type list_label: string
        :param item_value: the item that will be removed from the list or a list of items that will be removed
        :param update_timestamp_label: the label of the *last_update* field of the record if the last update timestamp
          must be recorded or None
        :type update_timestamp_label: field label or None
        :return: the timestamp of the last update as saved in the DB or None (if update_timestamp_field was None)
        """
        if isinstance(item_value, list):
            update_statement = {'$pullAll': {list_label: item_value}}
        else:
            update_statement = {'$pull': {list_label: item_value}}
        if update_timestamp_label:
            update_statement, last_update = self._update_record_timestamp(update_timestamp_label,
                                                                          update_statement)
        else:
            last_update = None
        if increase_version:
            update_statement = self._increase_version(update_statement)
        self._update_record(record_id, update_statement)
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
            '!=': '$ne',
            '>': '$gt',
            '>=': '$gte',
            '<': '$lt',
            '<=': '$lte'
        }
        right = cast_right_operand(right.strip())
        left = left.strip()
        if operand == '=':
            return {left: right}
        elif operand in operands_map:
            return {left: {operands_map[operand]: right}}
        else:
            raise ValueError('The operand %s is not supported' % operand)

    def _parse_expression(self, expression):
        # replace all invalid characters
        for not_allowed_char, allowed_char in self.ENCODINGS_MAP.iteritems():
            expression = expression.replace(not_allowed_char, allowed_char)
        q = expression.replace('/', '.')
        return q

    def _parse_simple_expression(self, expression):
        return super(MongoDriverPM2, self)._parse_simple_expression(expression)

    def _parse_match_expression(self, expr):
        return super(MongoDriverPM2, self)._parse_match_expression(expr)

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

    def _build_paths(self, containment_mapping):
        return super(MongoDriverPM2, self)._build_paths(containment_mapping)

    def _extract_path_alias(self, path):
        return super(MongoDriverPM2, self)._extract_path_alias(path)

    def _get_archetype_class_path(self, path):
        path_pieces = path.split('.')
        path_pieces[-1] = 'archetype_class'
        return '.'.join(path_pieces)

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
            or_statement = {'$or': [expressions[i] for i in or_indices]}
            expressions[max(expressions.keys())+1] = or_statement
            and_indices.append(max(expressions.keys()))
        if len(and_indices) > 0:
            for ai in and_indices:
                query.update(expressions[ai])
        else:
            for e in expressions.values():
                query.update(e)
        return query

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
                        query[lo] = ro
            else:
                raise PredicateException("No predicate expression found")
        elif type(predicate) == ArchetypePredicate:
            predicate_string = predicate.archetype_id
            query[predicate_string] = {'$exists': True}
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
                    query.update({'patient_id': right_operand})
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

    def _map_ehr_selection(self, path, ehr_var):
        path = path.replace('%s.' % ehr_var, '')
        if path == 'ehr_id.value':
            return {'patient_id': True}
        if path == 'uid.value':
            return {'_id': True}

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

    def _run_aql_query(self, query, fields, aliases, collection):
        self.logger.debug("Running query\n%s\nwith filters\n%s", query, fields)
        rs = ResultSet()
        for path, alias in aliases.iteritems():
            col = ResultColumnDef(alias, path)
            rs.add_column_definition(col)
        if self.is_connected:
            original_collection = self.collection_name
            close_conn_after_done = False
        else:
            close_conn_after_done = True
        self.connect()
        self.select_collection(collection)
        query_results = self.get_records_by_query(query, fields)

        if close_conn_after_done:
            self.disconnect()
        else:
            self.select_collection(original_collection)
        for q in query_results:
            record = dict()
            for x in self._split_results(q):
                record[x[0]] = x[1]
            rr = ResultRow(record)
            rs.add_row(rr)
        return rs

    def build_queries(self, query_model, patients_repository, ehr_repository, query_params=None):
        return super(MongoDriverPM2, self).build_queries(query_model, patients_repository, ehr_repository,
                                                      query_params)

    def _get_query_hash(self, query):
        return super(MongoDriverPM2, self)._get_query_hash(query)

    def _get_queries_hash_map(self, queries):
        return super(MongoDriverPM2, self)._get_queries_hash_map(queries)

    def _get_structures_hash_map(self, queries):
        return super(MongoDriverPM2, self)._get_structures_hash_map(queries)

    def _get_structures_selector(self, structure_ids):
        if len(structure_ids) == 1:
            return {'ehr_structure_id': structure_ids[0]}
        else:
            return {'ehr_structure_id': {'$in': structure_ids}}

    def _aggregate_queries(self, queries):
        aggregated_queries = list()
        queries_hash_map = self._get_queries_hash_map(queries)
        structures_hash_map = self._get_structures_hash_map(queries)
        for qhash, structures in structures_hash_map.iteritems():
            query = queries_hash_map[qhash]
            query['condition'].update(self._get_structures_selector(structures))
            aggregated_queries.append(query)
        return aggregated_queries

    def _get_query_hash_by_section(self, query, section):
        query_hash = md5()
        query_hash.update(json.dumps(query[section]))
        return query_hash.hexdigest()

    def _get_selection_maps(self, queries):
        selections_map = dict()
        queries_by_sel_map = dict()
        for q in queries:
            q_hash = self._get_query_hash_by_section(q, 'selection')
            queries_by_sel_map.setdefault(q_hash, list()).append(q)
            if q_hash not in selections_map:
                selections_map[q_hash] = q['selection']
        return selections_map, queries_by_sel_map

    def _aggregate_queries_by_selection(self, queries):
        aggregated_queries = list()
        sel_map, queries_map = self._get_selection_maps(queries)
        for sel_hash, mapped_queries in queries_map.iteritems():
            condition = {'$or': [q['condition'] for q in mapped_queries]}
            aggregated_queries.append({
                'condition': condition,
                'aliases': mapped_queries[0]['aliases'],
                'selection': sel_map[sel_hash]
            })
        return aggregated_queries

    def _find_by_aql_queries(self, queries, ehr_repository, query_processes):
        if len(queries) > 1:
            queries = self._aggregate_queries_by_selection(queries)
        total_results = ResultSet()
        if query_processes == 1 or len(queries) == 1:
            for query in queries:
                results = self._run_aql_query(query=query['condition'], fields=query['selection'],
                                              aliases=query['aliases'], collection=ehr_repository)
                total_results.extend(results)
        else:
            queries_pool = Pool(query_processes)
            results = queries_pool.imap_unordered(
                MultiprocessQueryRunnerPM2(self.host, self.database_name,
                                        ehr_repository, self.port, self.user, self.passwd),
                queries
            )
            for r in results:
                total_results.extend(r)
        return total_results

    def _count_by_aql_queries(self, queries, ehr_repository):
        if self.is_connected:
            original_collection = self.collection_name
            close_conn_after_done = False
        else:
            close_conn_after_done = True
        self.connect()
        self.select_collection(ehr_repository)
        if len(queries) == 1:
            results_counter = self.count_records_by_query(queries[0])
        else:
            results_counter = self.count_records_by_query({'$or': queries})
        if close_conn_after_done:
            self.disconnect()
        else:
            self.select_collection(original_collection)
        return results_counter

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
        if not count_only:
            return self._find_by_aql_queries(aggregated_queries, ehr_repository, query_processes)
        else:
            return self._count_by_aql_queries([aq['condition'] for aq in aggregated_queries],
                                              ehr_repository)
