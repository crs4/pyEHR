from openehr.aql.parser import *
from openehr.ehr.services.dbmanager.drivers.interface import DriverInterface
from openehr.ehr.services.dbmanager.querymanager.query import *
from openehr.ehr.services.dbmanager.errors import *
from openehr.utils import *
import pymongo
from bson.objectid import ObjectId
import re


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

    def add_record(self, record):
        """
        Save a record within MongoDB and return the record's ID
        """
        self.__check_connection()
        return self.collection.insert(record)

    def add_records(self, records):
        self.__check_connection()
        return super(MongoDriver, self).add_records(records)

    def get_record_by_id(self, record_id):
        self.__check_connection()
        return self.collection.find_one({'_id': ObjectId(record_id)})

    def get_all_records(self):
        self.__check_connection()
        return (rec for rec in self.collection.find())

    def delete_record(self, record_id):
        self.__check_connection()
        self.logger.debug('deleting document with ID %s', record_id)
        res = self.collection.remove(ObjectId(record_id))
        self.logger.debug('deleted %d documents', res[u'n'])

    @property
    def documents_count(self):
        self.__check_connection()
        return self.collection.count()

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