from openehr.aql.parser import *
from openehr.ehr.services.dbmanager.drivers.interface import DriverInterface
from openehr.ehr.services.dbmanager.querymanager.query import *
from openehr.ehr.services.dbmanager.errors import *
from openehr.utils import *
import pymongo
import pymongo.errors
from bson.objectid import ObjectId
import re
import time


class MongoDriver(DriverInterface):

    def __init__(self, host, database, collection,
                 port=None, user=None, passwd=None,
                 logger=None):
        self.client = None
        self.database = None
        self.collection = None
        self.host = host
        self.database_name = database
        self.collection_name = collection
        self.port = port
        self.user = user
        self.passwd = passwd
        self.logger = logger or get_logger('mongo-db-driver')

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.disconnect()
        return None

    def connect(self):
        """
        You can use connect and disconnect methods
        >>> driver = MongoDriver('localhost', 'test_database',
        ...                       'test_collection')
        >>> driver.connect()
        >>> driver.is_connected
        True
        >>> driver.disconnect()

        or you can use the context manager
        >>> with MongoDriver('localhost', 'test_database', 'test_collection') as driver:
        ...   driver.is_connected
        True

        """
        if not self.client:
            self.logger.debug('connecting to host %s', self.host)
            self.client = pymongo.MongoClient(self.host, self.port)
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
        You can use connect and disconnect methods
        >>> driver = MongoDriver('localhost', 'test_database',
        ...                      'test_collection')
        >>> driver.connect()
        >>> driver.disconnect()
        >>> driver.is_connected
        False

        or you can use the context manager
        >>> with MongoDriver('localhost', 'test_database', 'test_collection') as driver:
        ...   pass
        >>> driver.is_connected
        False

        """
        self.logger.debug('disconnecting from host %s', self.client.host)
        self.client.disconnect()
        self.database = None
        self.collection = None
        self.client = None

    @property
    def is_connected(self):
        return not self.client is None

    def __check_connection(self):
        if not self.is_connected:
            raise DBManagerNotConnectedError('Connection to host %s is closed' % self.host)

    def select_collection(self, collection_label):
        """
        Change the collection for the current database

        >>> with MongoDriver('localhost', 'test_database', 'test_collection_1') as driver:
        ...   print driver.collection
        ...   driver.select_collection('test_collection_2')
        ...   print driver.collection
        Collection(Database(MongoClient('localhost', 27017), u'test_database'), u'test_collection_1')
        Collection(Database(MongoClient('localhost', 27017), u'test_database'), u'test_collection_2')
        """
        self.__check_connection()
        self.logger.debug('Changing collection for database %s, old collection: %s - new collection %s',
                          self.database.name, self.collection.name, collection_label)
        self.collection = self.database[collection_label]

    def add_record(self, record):
        """
        Save a record within MongoDB and return the record's ID

        >>> record = {'_id': ObjectId('%023d%d' % (0, 1)), 'field1': 'value1', 'field2': 'value2'}
        >>> with MongoDriver('localhost', 'test_database', 'test_collection') as driver:
        ...   record_id = driver.add_record(record)
        ...   print repr(record_id)
        ...   driver.documents_count == 1
        ...   driver.delete_record(record_id) # cleanup
        ObjectId('000000000000000000000001')
        True
        """
        self.__check_connection()
        try:
            return self.collection.insert(record)
        except pymongo.errors.DuplicateKeyError:
            raise DuplicatedKeyError('A record with ID %s already exists' % record['_id'])

    def add_records(self, records):
        """
        >>> records = [
        ...   {'_id': ObjectId('%024d' % 1), 'field1': 'value1', 'field2': 'value2'},
        ...   {'_id': ObjectId('%024d' % 2), 'field1': 'value1', 'field2': 'value2'},
        ...   {'_id': ObjectId('%024d' % 3), 'field1': 'value1', 'field2': 'value2'},
        ... ]
        >>> with MongoDriver('localhost', 'test_database', 'test_collection') as driver:
        ...   records_id = driver.add_records(records)
        ...   for rid in records_id:
        ...     print repr(rid)
        ...   driver.documents_count == 3
        ...   for r in driver.get_all_records():
        ...     driver.delete_record(r['_id']) # cleanup
        ObjectId('000000000000000000000001')
        ObjectId('000000000000000000000002')
        ObjectId('000000000000000000000003')
        True
        """
        self.__check_connection()
        return super(MongoDriver, self).add_records(records)

    def get_record_by_id(self, record_id):
        """
        >>> with MongoDriver('localhost', 'test_database', 'test_collection') as driver:
        ...   record_id = driver.add_record({'field1': 'value1', 'field2': 'value2'})
        ...   rec = driver.get_record_by_id(record_id)
        ...   print rec['field1'], rec['field2']
        ...   driver.delete_record(record_id)
        value1 value2
        """
        self.__check_connection()
        res = self.collection.find_one({'_id': record_id})
        if res:
            return decode_dict(res)
        else:
            return res

    def get_all_records(self):
        self.__check_connection()
        return (decode_dict(rec) for rec in self.collection.find())

    def get_records_by_value(self, field, value):
        return self.get_records_by_query({field: value})

    def get_records_by_query(self, selector):
        """
        >>> records = []
        >>> for x in xrange(10):
        ...   records.append({'value': x, 'even': True if x%2 == 0 else False, '_id': ObjectId('%024d' % x)})
        >>> with MongoDriver('localhost', 'test_database', 'test_collection') as driver:
        ...   records_id = driver.add_records(records)
        ...   for r in driver.get_records_by_query({'even': True}):
        ...     print r
        ...   for rec_id in records_id:
        ...     driver.delete_record(rec_id)
        {'even': True, '_id': ObjectId('000000000000000000000000'), 'value': 0}
        {'even': True, '_id': ObjectId('000000000000000000000002'), 'value': 2}
        {'even': True, '_id': ObjectId('000000000000000000000004'), 'value': 4}
        {'even': True, '_id': ObjectId('000000000000000000000006'), 'value': 6}
        {'even': True, '_id': ObjectId('000000000000000000000008'), 'value': 8}
        """
        self.__check_connection()
        return (decode_dict(rec) for rec in self.collection.find(selector))

    def delete_record(self, record_id):
        self.__check_connection()
        self.logger.debug('deleting document with ID %s', record_id)
        res = self.collection.remove(record_id)
        self.logger.debug('deleted %d documents', res[u'n'])

    def update_record(self, record_id, update_condition):
        """
        >>> with MongoDriver('localhost', 'test_database', 'test_collection') as driver:
        ...   rec_id = driver.add_record({'label': 'label', 'field1': 'value1', 'field2': 'value2'})
        ...   print driver.get_record_by_id(rec_id)['label']
        ...   driver.update_record(rec_id, {'$set': {'label': 'new_label'}})
        ...   print driver.get_record_by_id(rec_id)['label']
        ...   driver.delete_record(rec_id)
        label
        new_label
        """
        self.__check_connection()
        self.logger.debug('Updading record with ID %r, with condition %r', record_id,
                          update_condition)
        res = self.collection.update({'_id': record_id}, update_condition)
        self.logger.debug('updated %d documents', res[u'n'])

    @property
    def documents_count(self):
        self.__check_connection()
        return self.collection.count()

    def _update_record_timestamp(self, timestamp_field, update_statement):
        last_update = time.time()
        update_statement.setdefault('$set', {})[timestamp_field] = last_update
        self.logger.debug('Update statement is %r', update_statement)
        return update_statement, last_update

    def update_field(self, record_id, field_label, field_value, update_timestamp_label=None):
        update_statement = {'$set': {field_label: field_value}}
        if update_timestamp_label:
            update_statement, last_update = self._update_record_timestamp(update_timestamp_label,
                                                                          update_statement)
        else:
            last_update = None
        self.update_record(record_id, update_statement)
        return last_update

    def add_to_list(self, record_id, list_label, item_value, update_timestamp_label=None):
        update_statement = {'$addToSet': {list_label: item_value}}
        if update_timestamp_label:
            update_statement, last_update = self._update_record_timestamp(update_timestamp_label,
                                                                          update_statement)
        else:
            last_update = None
        self.update_record(record_id, update_statement)
        return last_update

    def remove_from_list(self, record_id, list_label, item_value, update_timestamp_label=None):
        update_statement = {'$pull': {list_label: item_value}}
        if update_timestamp_label:
            update_statement, last_update = self._update_record_timestamp(update_timestamp_label,
                                                                          update_statement)
        else:
            last_update = None
        self.update_record(record_id, update_statement)
        return last_update

    def parseExpression(self, expression):
        q = expression.replace('/','.')
        return q

    def parseSimpleExpression(self, expression):
        expr = {}
        operator = re.search('>|>=|=|<|<=|!=', expression)
        if operator:
            op1 = expression[0:operator.start()].strip('\'')
            op2 = expression[operator.end():].strip('\'')
            op = expression[operator.start():operator.end()]
            if re.match('=', op):
                expr[op1] = op2
            elif re.match('!=', op):
                expr[op1] = {'$ne' : op2}
            elif re.match('>', op):
                expr[op1] = {'$gt' : op2}
            elif re.match('>=', op):
                expr[op1] = {'$gte' : op2}
            elif re.match('<', op):
                expr[op1] = {'$lt' : op2}
            elif re.match('<=', op):
                expr[op1] = {'$lte' : op2}
            else:
                raise ParseSimpleExpressionException("Invalid operator")
        else:
            q = expression.replace('/','.')
            expr[q] = {'$exists' : True}
        return expr

    def parseMatchExpression(self, expr):
        range = expr.expression.lstrip('{')
        range = range.rstrip('}')
        values = range.split(',')
        final = []
        for val in values:
            v = val.strip('\'')
            final.append(v)
        return final

    def calculateConditionExpression(self, query, condition):
        i = 0
        or_expressions = []
        while i < len(condition.conditionSequence):
            expression = condition.conditionSequence[i]
            if isinstance(expression, ConditionExpression):
                print "Expression: " + expression.expression
                op1 = self.parseExpression(expression.expression)
                if not i+1==len(condition.conditionSequence):
                    operator = condition.conditionSequence[i+1]
                    if isinstance(operator, ConditionOperator):
                        if operator.op == "AND":
                            if condition.conditionSequence[i+2].beginswith('('):
                                op2 = self.mergeExpr(condition.conditionSequence[i+2:])
                            else:
                                op2 = self.mergeExpr(condition.conditionSequence[i+2:])
                            expr = {"$and" : {op1, op2}}
                            or_expressions.append(expr)
                            i = i+3
                        elif operator.op == "OR":
                            or_expressions.append(op1)
                            i = i+2
                        elif operator.op == "MATCHES":
                            match = self.parseMatchExpression(condition.conditionSequence[i+2])
                            expr = {op1 : {"$in" : match}}
                            or_expressions.append(expr)
                            i = i+3
                        elif operator.op == ">":
                            expr = {op1 : {"$gt" : {condition.conditionSequence[i+2].expression}}}
                            or_expressions.append(expr)
                            i = i+3
                        elif operator.op == "<":
                            expr = {op1 : {"$lt" : {condition.conditionSequence[i+2].expression}}}
                            or_expressions.append(expr)
                            i = i+3
                        elif operator.op == "=":
                            expr = {op1 : {"$eq" : {condition.conditionSequence[i+2].expression}}}
                            or_expressions.append(expr)
                            i = i+3
                        elif operator.op == ">=":
                            expr = {op1 : {"$gte" : {condition.conditionSequence[i+2].expression}}}
                            or_expressions.append(expr)
                            i = i+3
                        elif operator.op == "<=":
                            expr = {op1 : {"$lte" : {condition.conditionSequence[i+2].expression}}}
                            or_expressions.append(expr)
                            i = i+3
                        else:
                            pass
                        print "Operator: " + operator.op
                    else:
                        pass
                else:
                    or_expressions.append(self.parseSimpleExpression(op1))
                    i += 1
        if len(or_expressions) == 1:
            print "or_expression single: " + str(or_expressions[0])
            query.update(or_expressions[0])
        else:
            print "or_expression: " + str(or_expressions)
            query["$or"] = or_expressions

    def computePredicate(self, query, predicate):
        if isinstance(predicate, PredicateExpression):
            predEx = predicate.predicateExpression
            if predEx:
                lo = predEx.leftOperand
                if not lo:
                    raise PredicateException("MongoDriver.computePredicate: No left operand found")
                op = predEx.operand
                ro = predEx.rightOperand
                if op and ro:
                    print "lo: %s - op: %s - ro: %s" % (lo, op, ro)
                    if op == "=":
                        query[lo] = ro
            else:
                raise PredicateException("MongoDriver.computePredicate: No predicate expression found")
        elif isinstance(predicate, ArchetypePredicate):
            predicateString = predicate.archetypeId
            query[predicateString] = {'$exists' : True}
        else:
            raise PredicateException("MongoDriver.computePredicate: No predicate expression found")

    def calculateLocationExpression(self, query, location):
        # Here is where the collection has been chosen according to the selection
        print "LOCATION: %s" % str(location)
        if location.classExpression:
            ce = location.classExpression
            className = ce.className
            variableName = ce.variableName
            predicate = ce.predicate
            if predicate:
                self.computePredicate(query, predicate)
        else:
            raise Exception("MongoDriver Exception: Query must have a location expression")

        for cont in location.containers:
            if cont.classExpr:
                ce = cont.classExpr
                className = ce.className
                variableName = ce.variableName
                predicate = ce.predicate
                if predicate:
                    self.computePredicate(query, predicate)
        print "QUERY: %s" % query
        print (self.collection)
        resp = self.collection.find(query)
        print resp.count()

    def createResponse(self, dbQuery, selection):
        # execute the query
        print "QUERY PRE: %s" % str(dbQuery)
        # Prepare the response
        rs = ResultSet()
        # Declaring a projection to retrieve only the selected fields
        proj = {}
        proj['_id'] = 0
        for var in selection.variables:
            columnDef = ResultColumnDef()
            columnDef.name = var.label
            columnDef.path = var.variable.path.value
            rs.columns.append(columnDef)
            projCol = columnDef.path.replace('/','.').strip('.')
            proj[projCol] = 1
        print "PROJ: %s" % str(proj)
        queryResult = self.collection.find(dbQuery, proj)
        rs.totalResults = queryResult.count()
        for q in queryResult:
            rr = ResultRow()
            rr = ResultRow()
            rr.items = q.values()
            rs.rows.append(rr)
        return rs

    def executeQuery(self, query):
        self.__check_connection()
        try:
            selection = query.selection
            location = query.location
            condition = query.condition
            orderRules = query.orderRules
            timeConstraints = query.timeConstraints
            dbQuery = {}
            # select the collection
            self.calculateLocationExpression(dbQuery,location)
            # prepare the query to the db
            if condition:
                self.calculateConditionExpression(dbQuery,condition)
            # create the response
            return self.createResponse(dbQuery, selection)
        except Exception, e:
            print "Mongo Driver Error: " + str(e)
            return None