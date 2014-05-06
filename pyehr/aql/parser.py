import re
from pyehr.aql.model import *

class InvalidAQLError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class ParsePathError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class ParsingError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class ParseSelectionError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class ParseLocationError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class ParseConditionError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class ParsePredicateExpressionError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class ParseOrderRulesError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class ParseTimeConstraintsError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class Parser():

    keywords = ('EHR',
                'COMPOSITION',
                'OBSERVATION',
                'CONTAINS')

    def __init__(self):
        self.selection = None
        self.location = None
        self.condition = None
        self.order_rules = None
        self.time_constraints = None

    def parse(self, statement):
        try:
            text = statement.strip()
            if not re.match('SELECT ', text.upper()):
                raise InvalidAQLError('AQL statements must begin with the SELECT keyword')
                return

            result = re.search(' FROM ', text.upper())
            if not result:
                raise InvalidAQLError('AQL statements must contain the FROM clause')
                return
            else:
                self.selection = text[7:result.start()]
                location_start = result.start()+6
                optionResult = re.search(' WHERE | ORDER BY | TIMEWINDOW ', text.upper())
                if optionResult:
                    location_end = optionResult.start()
                    self.location = text[location_start:location_end]
                    optionalText = text[location_end:]
                    whereResult = re.search(' WHERE ', optionalText.upper())
                    if whereResult:
                        #print optionalText
                        condition_start = whereResult.start()+7
                        otherOptionResult = re.search(' ORDER BY | TIMEWINDOW ', optionalText.upper())
                        if otherOptionResult and otherOptionResult.start()>condition_start:
                            condition_stop = otherOptionResult.start()
                            self.condition = optionalText[condition_start:condition_stop]
                        else:
                            self.condition = optionalText[condition_start:]
                    orderResult = re.search(' ORDER BY ', optionalText.upper())
                    if orderResult:
                        order_start = orderResult.start()+7
                        otherOptionResult = re.search(' WHERE | TIMEWINDOW ', optionalText.upper())
                        if otherOptionResult and otherOptionResult.start()>order_start:
                            order_stop = otherOptionResult.start()
                            self.order_rules = optionalText[order_stop:condition_stop]
                        else:
                            self.order_rules = optionalText[order_start:]
                    timeResult = re.search(' TIMEWINDOW ', optionalText.upper())
                    if timeResult:
                        time_start = timeResult.start()+7
                        otherOptionResult = re.search(' WHERE | ORDER BY ', optionalText.upper())
                        if otherOptionResult and otherOptionResult.start()>time_start:
                            time_stop = otherOptionResult.start()
                            self.time_constraints = optionalText[time_start:time_stop]
                        else:
                            self.time_constraints = optionalText[time_start:]
                else:
                    self.location = text[location_start:]

        except Exception as e:
            print "Parse Error: %s" % str(e)
            raise ParsingError(e)

        query = QueryModel()
        # In order to retrieve the variable list the location expression must be parsed first
        query.selection = self.parse_selection(self.selection)
        query.location = self.parse_location(self.location)
        if self.condition:
            query.condition = self.parse_condition(self.condition)
        if self.order_rules:
            query.orderRules = self.parse_order_rules(self.order_rules)
        if self.time_constraints:
            query.timeConstraints = self.parse_time_constraints(self.time_constraints)
        return query

    ''' This function return a predicate object, given a string
        AQL has three types of Predicates: standard predicate, archetype predicate, and node predicate.

        Standard predicate always has left operand, operator and right operand, e.g. [ehr_id/value='123456']:
         - left operand is normally an openEHR path, such as ehr_id/value, name/value
         - right operand is normally a criteria value or a parameter, such as '123456', $ehrUid. It can also be an openEHR path (based on the BNF), but we do not have an example of this situation yet.
         - operators include: >, >=, =, <, <=, !=

        Archetype predicate is a shortcut of standard predicate, i.e. the predicate does not have left operand and operator. It only has an archetype id, e.g. [openEHR-EHR-COMPOSITION.encounter.v1].
        Archetype predicate is a specific type of query criteria indicating what archetype instances are relevant to this query.
        It is used to scope the the data source from which the query expected data is to be retrieved. Therefore, an archetype predicate is only used within an AQL FROM clause

        Node predicate is also a shortcut of standard predicate. It has the following forms:
         - containing an archetype node id (known as atcode) only;
         - containing an archetype node id and a name value criteria;
         - containing an archetype node id and a shortcut of name value criteria;
         - The above three forms are the most common node predicates. A more advanced form is to include a general criteria instead of the name/value criteria within the predicate. The general criteria consists of left operand, operator, and right operand.
        Node predicate defines criteria on fine-grained data. It is only used within an identified path.
    '''
    def parsePredicateExpression(self, expression):
        if expression:
            predicateExpr = PredicateExpression()
            operator = re.search('>|>=|=|<|<=|!=', expression)
            if operator:
                predicateExpr.leftOperand = expression[1:operator.start()]
                predicateExpr.operand = expression[operator.start():operator.end()]
                predicateExpr.rightOperand = expression[operator.end():len(expression)-1]
            else:
                predicateExpr.leftOperand = expression
            return predicateExpr
        else:
            raise ParsePredicateExpressionError("parsePredicateExpression: No valid expression found")

    def parsePredicate(self, predicateString):
        operator = re.search('>|>=|=|<|<=|!=', predicateString)
        predicate = None
        if operator:
            # is a Standard predicate
            tokens = predicateString.split()
            if len(tokens) > 1:
                predicate = NodePredicate()
                for token in tokens:
                    predicate.predicateExpressionList.append(self.parsePredicateExpression(token))
            else:
                predicate = Predicate()
                predicate.predicateExpression = self.parsePredicateExpression(predicateString)
        else:
            # If the expression doesn't contain an operator, it means that is an Archetype predicate
            predicate = ArchetypePredicate()
            predicate.archetypeId = predicateString[1:len(predicateString)-1]
        return predicate

    def parsePath(self, pathString):
        path = Path()
        tokenList = pathString.lstrip('/').split('/')
        for token in tokenList:
            node = NodePath()
            predicateStart = re.search('\[', token)
            predicateEnd = re.search('\]', token)
            if predicateStart and  predicateEnd:
                node.attributeName = token[0:predicateStart.start()]
                node.predicateValue = self.parsePredicate(token[predicateStart.start()+1:predicateEnd.start()-1])
            else:
                node.attributeName = token
            path.nodeList.append(node)
        path.value = pathString
        return path

    # These functions are defined to parse the selection part of the query

    def parseIdentifiedPath(self, identifiedPathString):
        var = None
        path = IdentifiedPath()
        sr = re.search('/|\[', identifiedPathString)
        var = identifiedPathString[0:sr.start()]
        '''
        AQL identified path has the following forms:
        1 - consisting an AQL variable name defined within the FROM clause, followed by an openEHR path, e.g.
        2 - consisting an AQL variable name followed by a predicate, e.g.
        3 - consisting an AQL variable name followed by a predicate and an openEHR path, e.g.
        '''
        if var:
            path.variable = var
            str = identifiedPathString[len(var):]
            # calculating case 2 and 3
            if str.startswith('['):
                end = re.search(']', str)
                if end:
                    path.predicate = str[1:end.start()]
                    path.path = self.parsePath(str[end.start()+1:])
            else:
                # case 1
                path.path = self.parsePath(str)
            return path
        else:
            raise ParsePathError("An error occured while parsing the path: "+identifiedPathString)

    def parse_selection(self, sel):
        try:
            #print "variableList: " + str(variableList)
            selection = Selection()
            topResult = re.match('TOP ', sel.upper())
            class_list = sel
            topNumber = 0
            if topResult:
                topSplit = sel.split(' ')
                topNumberString = topSplit[1]
                topNumber = int(topNumberString)
                selection.top = topNumber
                topNumberLenght = len(topNumberString)
                class_list = sel[4+topNumberLenght :]
            try:
                classes = class_list.split(',')
                print("CLASSLIST: %s" % class_list)
                for cl in classes:
                    variable = Variable()
                    class_tokens = cl.strip().split(" ")
                    if class_tokens and len(class_tokens) == 3:
                        variable.variable = self.parseIdentifiedPath(class_tokens[0])
                        variable.label = class_tokens[2]
                    else:
                        variable.variable = self.parseIdentifiedPath(cl)
                    selection.variables.append(variable)
            except Exception, ex:
                print "ERROR: %s" % ex
                variable = Variable()
                class_tokens = class_list.strip().split(" ")
                variable.variable = self.parseIdentifiedPath(class_list)
                if class_tokens and len(class_tokens) == 3:
                    variable.label = class_tokens[2]
                selection.variables.append(variable)
            return selection
        except Exception, e:
            print "Error: " + str(e)
            raise ParseSelectionError(str(e))

    # These functions are defined to parse the location part of an AQL statement

    def parseClassExpression(self, text):
        mObj = re.match('EHR |COMPOSITION |OBSERVATION ', text.upper())
        if mObj:
            classExpr = ClassExpression()
            end = mObj.end()
            classExpr.className = text[0:end]
            optionalText = text[end:]
            tokens = optionalText.split()
            # Looking for the optional parts...
            # If it starts with [ it means is a predicate expression...
            if tokens[0].startswith('['):
                classExpr.predicate = self.parsePredicate(tokens[0].lstrip('[').rstrip(']'))
            else:
            #... otherwise is a variable definition...
                pred = re.search('\[',tokens[0])
                if pred:
                    #... followed by a predicate expression.
                    classExpr.variableName = tokens[0][0:pred.start()]
                    classExpr.predicate = self.parsePredicate(tokens[0][pred.start():])
                else:
                    #... without a predicate expression.
                    classExpr.variableName = tokens[0]
                if len(tokens) > 1:
                    classExpr.predicate = self.parsePredicate(tokens[1].lstrip('[').rstrip(']'))
            return classExpr
        else:
            print "parseClassExpression ERROR"
            raise Exception("parseClassExpression ERROR")

    def parseContainers(self, text):
        #c = re.search(' CONTAINS ', text.upper())
        conts = list(re.finditer('CONTAINS ', text.upper()))
        containers = []
        for i in xrange(len(conts)):
            c = conts[i]
            start = c.start()
            if i<len(conts)-1:
                end = conts[i+1].start()
                txt = text[9+start:end]
            else:
                txt = text[9+start:]
            classExpr = self.parseClassExpression(txt)
            container = Container()
            container.classExpr = classExpr
            containers.append(container)
        return containers

    def parse_location(self, locationString):
        '''
        The FROM clause utilises class expressions and a set of containment criteria to specify the data source
        from which the query required data is to be retrieved.
        Its function is similar as the FROM clause of an SQL expression.
        '''
        try:
            # A simple FROM clause consists of three parts: keyword - FROM, class expression and/or containment constraints.
            #
            # Checking the keyword expression
            mObj = re.match('EHR |COMPOSITION |OBSERVATION ', locationString.upper())
            leaves = []
            if mObj:
                location = Location()
                # Looking for containment expressions
                c = re.search(' CONTAINS ', locationString.upper())
                containment = None
                if c:
                    cpos = c.start()
                    # retrieving the containment expression
                    containment = locationString[cpos: ]
                    location.containers = self.parseContainers(containment)
                    # retrieving the class expression
                    classExpr = locationString[0:cpos]
                else:
                    # retrieving the class expression
                    classExpr = locationString
                location.classExpression = self.parseClassExpression(classExpr)
                return location
            else:
                raise InvalidAQLError('A class expression must have an openEHR RM class name, such as EHR, COMPOSITION, OBSERVATION etc.')
                return None
        except Exception, e:
            print "An error occurred while parsing the location: " + str(e)
            raise ParseLocationError(e)
            return None

    # Parse the condition part of an AQL statement
    def parse_condition(self, condition):
        try:
            typeKeywords = ("OR", "AND", "MATCHES", ">", "<", ">=", "<=", "=")
            cond = Condition()
            tokens = condition.split(" ")
            start = False
            block = False
            end = True
            buf = ""
            if len(tokens) <= 1:
                predExpr = self.parsePredicateExpression(tokens[0])
                cond.condition = predExpr
            else:
                condSeq = ConditionSequence()
                for token in tokens:
                    if token.upper() in typeKeywords:
                        op = ConditionOperator()
                        op.op = token.upper()
                        condSeq.conditionSequence.append(op)
                    else:
                        if token.startswith('{') or token.startswith('('):
                            start = True
                            block = True
                            end = False
                        if block:
                            buf += token
                        else:
                            buf = token
                        if token.endswith('}') or token.endswith(')'):
                            start = False
                            end = True
                        if end:
                            predExpr = self.parsePredicateExpression(buf)
                            condSeq.conditionSequence.append(predExpr)
                            buf = ""
                cond.condition = condSeq
            return cond
        except Exception, e:
            raise ParseConditionError(e.message)

    # TBD...
    def parse_order_rules(self, order_rules):
        try:
            order = OrderRules()
            return order
        except:
            raise ParseOrderRulesError

    # TBD...
    def parse_time_constraints(self, time_constraints):
        try:
            timeConstraints = TimeConstraints()
            return timeConstraints
        except:
            raise ParseTimeConstraintsError