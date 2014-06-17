from pyehr.aql.parser import *
from pyehr.aql.model import ConditionExpression
from pyehr.ehr.services.dbmanager.drivers.interface import DriverInterface
from pyehr.ehr.services.dbmanager.querymanager.query import ResultSet,\
    ResultColumnDef, ResultRow
from pyehr.ehr.services.dbmanager.errors import *
from pyehr.utils import *
import pymongo
import pymongo.errors
import re
import time


class MongoDriver(DriverInterface):
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
            self.logger.debug('Alredy connected to database %s, using collection %s',
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
        # MongoDB doesn't need structures initilization
        pass

    @property
    def is_connected(self):
        """
        Check if the connection to the MongoDB server is opened.

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
            'creation_time': clinical_record.creation_time,
            'last_update': clinical_record.last_update,
            'active': clinical_record.active,
            'ehr_data': ehr_data
        }
        if clinical_record.record_id:
            encoded_record['_id'] = clinical_record.record_id
        if structure_id:
            encoded_record['ehr_structure_id'] = structure_id
        return encoded_record

    def encode_record(self, record):
        """
        Encode a :class:`Record` object into a data structure that can be saved within
        MongoDB

        :param record: the record that must be encoded
        :type record: a :class:`Record` subclass
        :return: the record encoded as a MongoDB document
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
                record_id=record.get('_id'),
            )
        else:
            return PatientRecord(
                creation_time=record['creation_time'],
                record_id=record.get('_id')
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
            return ClinicalRecord(
                ehr_data=ArchetypeInstance.from_json(ehr_data),
                creation_time=record['creation_time'],
                last_update=record['last_update'],
                active=record['active'],
                record_id=record.get('_id')
            )
        else:
            if record.get('ehr_data'):
                arch = ArchetypeInstance(record['ehr_data']['archetype'], {})
            else:
                arch = None
            return ClinicalRecord(
                creation_time=record.get('creation_time'),
                record_id=record.get('_id'),
                last_update=record.get('last_update'),
                active=record.get('active'),
                ehr_data=arch
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
        self.__check_connection()
        try:
            return self.collection.insert(record)
        except pymongo.errors.DuplicateKeyError:
            raise DuplicatedKeyError('A record with ID %s already exists' % record['_id'])

    def add_records(self, records):
        """
        Save a list of records within MongoDB and return records' IDs

        :param records: the list of records that is going to be saved
        :type records: list
        :return: a list of records' IDs
        :rtype: list
        """
        self.__check_connection()
        return super(MongoDriver, self).add_records(records)

    def get_record_by_id(self, record_id):
        """
        Retrieve a record using its ID

        :param record_id: the ID of the record
        :return: the record of None if no match was found for the given record
        :rtype: dictionary or None
        """
        self.__check_connection()
        res = self.collection.find_one({'_id': record_id})
        if res:
            return decode_dict(res)
        else:
            return res

    def get_all_records(self):
        """
        Retrieve all records within current collection

        :return: all the records stored in the current collection
        :rtype: list
        """
        self.__check_connection()
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

    def get_records_by_query(self, selector, fields=None):
        """
        Retrieve all records matching the given query

        :param selector: the selector (in MongoDB syntax) used to select data
        :type selector: dictionary
        :param fields: a list of field names that should be returned in the result set or a dict specifying the
                       fields to include or exclude
        :type fields: list or dictionary
        :return: a list with the matching records
        :rtype: list
        """
        self.__check_connection()
        return (decode_dict(rec) for rec in self.collection.find(selector, fields))

    def delete_record(self, record_id):
        """
        Delete an existing record

        :param record_id: record's ID
        """
        self.__check_connection()
        self.logger.debug('deleting document with ID %s', record_id)
        res = self.collection.remove(record_id)
        self.logger.debug('deleted %d documents', res[u'n'])

    def _update_record(self, record_id, update_condition):
        """
        Update an existing record

        :param_record_id: record's ID
        :param update_condition: the update condition (in MongoDB syntax)
        :type update_condition: dictionary
        """
        self.__check_connection()
        self.logger.debug('Updading record with ID %r, with condition %r', record_id,
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
        self.__check_connection()
        return self.collection.count()

    def _update_record_timestamp(self, timestamp_field, update_statement):
        last_update = time.time()
        update_statement.setdefault('$set', {})[timestamp_field] = last_update
        self.logger.debug('Update statement is %r', update_statement)
        return update_statement, last_update

    def update_field(self, record_id, field_label, field_value, update_timestamp_label=None):
        """
        Update record's field *field* with given value

        :param record_id: record's ID
        :param field_label: field's label
        :type field_label: string
        :param field_value: new value for the selected field
        :param update_timestamp_label: the label of the *last_update* field of the record if the last update timestamp
          must be recorded or None
        :type update_timestamp_label: field label or None
        :return: the timestamp of the last update as saved in the DB or None (if update_timestamp_field was None)
        """
        update_statement = {'$set': {field_label: field_value}}
        if update_timestamp_label:
            update_statement, last_update = self._update_record_timestamp(update_timestamp_label,
                                                                          update_statement)
        else:
            last_update = None
        self._update_record(record_id, update_statement)
        return last_update

    def add_to_list(self, record_id, list_label, item_value, update_timestamp_label=None):
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
        update_statement = {'$addToSet': {list_label: item_value}}
        if update_timestamp_label:
            update_statement, last_update = self._update_record_timestamp(update_timestamp_label,
                                                                          update_statement)
        else:
            last_update = None
        self._update_record(record_id, update_statement)
        return last_update

    def remove_from_list(self, record_id, list_label, item_value, update_timestamp_label=None):
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
        update_statement = {'$pull': {list_label: item_value}}
        if update_timestamp_label:
            update_statement, last_update = self._update_record_timestamp(update_timestamp_label,
                                                                          update_statement)
        else:
            last_update = None
        self._update_record(record_id, update_statement)
        return last_update

    def _parse_expression(self, expression):
        # replace all invalid characters
        for not_allowed_char, allowed_char in self.ENCODINGS_MAP.iteritems():
            expression = expression.replace(not_allowed_char, allowed_char)
        q = expression.replace('/', '.')
        return q

    def _parse_simple_expression(self, expression):
        expr = {}
        operator = re.search('>|>=|=|<|<=|!=', expression)
        if operator:
            op1 = expression[0:operator.start()].strip('\'')
            op2 = expression[operator.end():].strip('\'')
            op = expression[operator.start():operator.end()]
            if re.match('=', op):
                expr[op1] = op2
            elif re.match('!=', op):
                expr[op1] = {'$ne': op2}
            elif re.match('>', op):
                expr[op1] = {'$gt': op2}
            elif re.match('>=', op):
                expr[op1] = {'$gte': op2}
            elif re.match('<', op):
                expr[op1] = {'$lt': op2}
            elif re.match('<=', op):
                expr[op1] = {'$lte': op2}
            else:
                msg = 'Invalid operator in expression %s' % expression
                self.logger.error(msg)
                raise ParseSimpleExpressionException(msg)
        else:
            q = self._parse_expression(expression)
            expr[q] = {'$exists': True}
        return expr

    def _parse_match_expression(self, expr):
        values = expr.expression.lstrip('{').rstrip('}').split(',')
        final = [v.strip('\'') for v in values]
        return final

    def _calculate_condition_expression(self, query, condition):
        i = 0
        or_expressions = []
        while i < len(condition.condition_sequence):
            expression = condition.condition_sequence[i]
            if isinstance(expression, ConditionExpression):
                self.logger.debug("Expression: " + expression.expression)
                op1 = self._parse_expression(expression.expression)
                if not i+1 == len(condition.condition_sequence):
                    operator = condition.condition_sequence[i+1]
                    if isinstance(operator, ConditionOperator):
                        if operator.op == "AND":
                            if condition.condition_sequence[i+2].beginswith('('):
                                op2 = self.mergeExpr(condition.condition_sequence[i+2:])
                            else:
                                op2 = self.mergeExpr(condition.condition_sequence[i+2:])
                            expr = {"$and": {op1, op2}}
                            or_expressions.append(expr)
                            i += 3
                        elif operator.op == "OR":
                            or_expressions.append(op1)
                            i += 2
                        elif operator.op == "MATCHES":
                            match = self._parse_match_expression(condition.condition_sequence[i+2])
                            expr = {op1: {"$in" : match}}
                            or_expressions.append(expr)
                            i += 3
                        elif operator.op == ">":
                            expr = {op1: {"$gt": {condition.condition_sequence[i+2].expression}}}
                            or_expressions.append(expr)
                            i += 3
                        elif operator.op == "<":
                            expr = {op1: {"$lt": {condition.condition_sequence[i+2].expression}}}
                            or_expressions.append(expr)
                            i += 3
                        elif operator.op == "=":
                            expr = {op1: {"$eq": {condition.condition_sequence[i+2].expression}}}
                            or_expressions.append(expr)
                            i += 3
                        elif operator.op == ">=":
                            expr = {op1: {"$gte": {condition.conditionSequence[i+2].expression}}}
                            or_expressions.append(expr)
                            i += 3
                        elif operator.op == "<=":
                            expr = {op1: {"$lte": {condition.conditionSequence[i+2].expression}}}
                            or_expressions.append(expr)
                            i += 3
                        else:
                            pass
                        print "Operator: " + operator.op
                    else:
                        pass
                else:
                    or_expressions.append(self._parse_simple_expression(op1))
                    i += 1
        if len(or_expressions) == 1:
            self.logger.debug("or_expression single: " + str(or_expressions[0]))
            query.update(or_expressions[0])
        else:
            self.logger.debug("or_expression: " + str(or_expressions))
            query["$or"] = or_expressions

    def _compute_predicate(self, query, predicate):
        if type(predicate) == Predicate:
            pred_ex = predicate.predicate_expression
            if pred_ex:
                lo = pred_ex.left_operand
                if not lo:
                    raise PredicateException("MongoDriver._compute_predicate: No left operand found")
                op = pred_ex.operand
                ro = pred_ex.right_operand
                if op and ro:
                    self.logger.debug("lo: %s - op: %s - ro: %s", lo, op, ro)
                    if op == "=":
                        query[lo] = ro
            else:
                raise PredicateException("MongoDriver._compute_predicate: No predicate expression found")
        elif type(predicate) == ArchetypePredicate:
            predicate_string = predicate.archetype_id
            query[predicate_string] = {'$exists': True}
        else:
            raise PredicateException("MongoDriver._compute_predicate: No predicate expression found")

    def _calculate_location_expression(self, query, location):
        # Here is where the collection has been chosen according to the selection
        self.logger.debug("LOCATION: %s", str(location))
        if location.class_expression:
            ce = location.class_expression
            class_name = ce.class_name
            variable_name = ce.variable_name
            predicate = ce.predicate
            if predicate:
                self._compute_predicate(query, predicate)
        else:
            raise Exception("MongoDriver Exception: Query must have a location expression")

        for cont in location.containers:
            if cont.class_expression:
                ce = cont.class_expression
                class_name = ce.class_name
                variable_name = ce.variable_name
                predicate = ce.predicate
                if predicate:
                    self._compute_predicate(query, predicate)
        self.logger.debug('Running query %s on collection %s', query, self.collection)
        resp = self.collection.find(query)
        self.logger.debug('results count = %d', resp.count())
        return resp

    def _create_response(self, db_query, selection):
        # execute the query
        self.logger.debug("QUERY PRE: %s", str(db_query))
        # Prepare the response
        rs = ResultSet()
        # Declaring a projection to retrieve only the selected fields
        proj = {'_id': False}
        for var in selection.variables:
            column_def = ResultColumnDef()
            column_def.name = var.label
            column_def.path = var.variable.path.value
            rs.columns.append(column_def)
            proj_col = column_def.path.replace('/', '.').strip('.')
            proj[proj_col] = True
        self.logger.debug("PROJ: %s", str(proj))
        # query_result = self.collection.find(db_query, proj)
        query_results = self.get_records_by_query(db_query, proj)
        rs.total_results = query_results.count()
        for q in query_results:
            rr = ResultRow()
            rr.items = q.values()
            rs.rows.append(rr)
        return rs

    def execute_query(self, query_model):
        self.__check_connection()
        try:
            selection = query_model.selection
            location = query_model.location
            condition = query_model.condition
            # order_rules = query_model.order_rules
            # time_constraints = query_model.time_constraints
            db_query = {}
            # select the collection
            self._calculate_location_expression(db_query, location)
            # prepare the query to the db
            if condition:
                self._calculate_condition_expression(db_query, condition)
            # create the response
            return self._create_response(db_query, selection)
        except Exception, e:
            self.logger.error("Mongo Driver Error: %s", str(e))
            return None