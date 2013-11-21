__author__ = 'ciccio'

from openehr.aql.parser import *
from openehr.ehr.services.dbmanager.drivers.interface import *
from openehr.ehr.services.dbmanager.querymanager.query import *
from openehr.ehr.services.dbmanager.errors import *
from openehr.utils import *
import pymongo
import re

class PredicateException(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class ParseSimpleExpressionException(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class MongoDriver(DriverInterface):

    def __init__(self, host, database, collection,
                 port=None, user=None, passwd=None,
                 logger=None):
        self.client = pymongo.MongoClient(host, port)
        print "Connecting to the Client"
        self.database = self.client[database]
        if user:
            self.database.authenticate(user, passwd)
        self.collection = self.database[collection]
        self.logger = logger or get_logger('mongo-db-driver')

    def connect(self):
        raise NotImplementedError()

    def close(self):
        raise NotImplementedError()

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