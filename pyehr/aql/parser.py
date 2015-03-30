import re
from errors import InvalidAQLError, ParsingError, ParsePredicateExpressionError,\
    ParsePathError, ParseSelectionError, ParseLocationError, ParseConditionError
from pyehr.aql.model import QueryModel, NodePredicate, Predicate, ArchetypePredicate, IdentifiedPath, Selection, \
    Variable, Path, NodePath, ClassExpression, Container, Location, Condition, ConditionSequence, ConditionOperator,\
    PredicateExpression
from pyehr.utils import get_logger


class Parser(object):

    KEYWORDS = ('EHR', 'COMPOSITION', 'OBSERVATION', 'CONTAINS')

    def __init__(self, logger=None):
        self.selection = None
        self.location = None
        self.condition = None
        self.order_rules = None
        self.time_constraints = None
        self.logger = logger or get_logger('pyehr-aql-parser')

    def reset(self):
        self.selection = None
        self.location = None
        self.condition = None
        self.order_rules = None
        self.time_constraints = None
        self.logger.debug('Parser resetted')

    def parse(self, statement):
        self.reset()
        try:
            text = statement.replace('\n', ' ').strip()
            if not re.match('SELECT ', text.upper()):
                raise InvalidAQLError('AQL statements must begin with the SELECT keyword')
            result = re.search(' FROM ', text.upper())
            if not result:
                raise InvalidAQLError('AQL statements must contain the FROM clause')
            else:
                self.selection = text[7:result.start()]
                location_start = result.start()+6
                option_result = re.search(' WHERE | ORDER BY | TIMEWINDOW ', text.upper())
                if option_result:
                    location_end = option_result.start()
                    self.location = text[location_start:location_end]
                    optional_text = text[location_end:]
                    where_result = re.search(' WHERE ', optional_text.upper())
                    if where_result:
                        condition_start = where_result.start()+7
                        other_option_result = re.search(' ORDER BY | TIMEWINDOW ', optional_text.upper())
                        if other_option_result and other_option_result.start() > condition_start:
                            condition_stop = other_option_result.start()
                            self.condition = optional_text[condition_start:condition_stop]
                        else:
                            self.condition = optional_text[condition_start:]
                    order_result = re.search(' ORDER BY ', optional_text.upper())
                    if order_result:
                        order_start = order_result.start()+7
                        other_option_result = re.search(' WHERE | TIMEWINDOW ', optional_text.upper())
                        if other_option_result and other_option_result.start() > order_start:
                            order_stop = other_option_result.start()
                            self.order_rules = optional_text[order_start:order_stop]
                        else:
                            self.order_rules = optional_text[order_start:]
                    time_result = re.search(' TIMEWINDOW ', optional_text.upper())
                    if time_result:
                        time_start = time_result.start()+7
                        other_option_result = re.search(' WHERE | ORDER BY ', optional_text.upper())
                        if other_option_result and other_option_result.start() > time_start:
                            time_stop = other_option_result.start()
                            self.time_constraints = optional_text[time_start:time_stop]
                        else:
                            self.time_constraints = optional_text[time_start:]
                else:
                    self.location = text[location_start:]
        except Exception as e:
            self.logger.error("Parse Error: %s" % str(e))
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

    def parse_predicate_expression(self, expression):
        """
        This function return a predicate object, given a string
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
        """
        if expression:
            predicate_expr = PredicateExpression()
            operator = re.search('>=|>|<=|<|!=|=', expression)
            if operator:
                predicate_expr.left_operand = expression[:operator.start()].strip()
                predicate_expr.operand = expression[operator.start():operator.end()].strip()
                predicate_expr.right_operand = expression[operator.end():].strip()
            else:
                predicate_expr.leftOperand = expression
            return predicate_expr
        else:
            raise ParsePredicateExpressionError("No valid expression found")

    def parse_predicate(self, predicate_string):
        operator = re.search('>=|>|<=|<|!=|=', predicate_string)
        if operator:
            # is a Standard predicate
            tokens = predicate_string.split()
            if len(tokens) > 1:
                predicate = NodePredicate()
                for token in tokens:
                    predicate.predicate_expression_list.append(self.parse_predicate_expression(token))
            else:
                predicate = Predicate()
                predicate.predicate_expression = self.parse_predicate_expression(predicate_string)
        else:
            # If the expression doesn't contain an operator, it means that is an Archetype predicate
            predicate = ArchetypePredicate()
            predicate.archetype_id = predicate_string[:len(predicate_string)].rstrip(']').lstrip('[')
        return predicate

    def parse_path(self, path_string):
        path = Path()
        token_list = path_string.lstrip('/').split('/')
        for token in token_list:
            node = NodePath()
            predicate_start = re.search('\[', token)
            predicate_end = re.search('\]', token)
            if predicate_start and predicate_end:
                node.attribute_name = token[0:predicate_start.start()]
                node.predicate_value = self.parse_predicate(token[predicate_start.start()+1:predicate_end.start()-1])
            else:
                node.attribute_name = token
            path.node_list.append(node)
        path.value = path_string
        return path

    # These functions are defined to parse the selection part of the query
    def parse_identified_path(self, identified_path_string):
        path = IdentifiedPath()
        sr = re.search('/|\[', identified_path_string)
        var = identified_path_string[0:sr.start()]

        # AQL identified path has the following forms:
        # 1 - consisting an AQL variable name defined within the FROM clause, followed by an openEHR path, e.g.
        # 2 - consisting an AQL variable name followed by a predicate, e.g.
        # 3 - consisting an AQL variable name followed by a predicate and an openEHR path, e.g.
        if var:
            path.variable = var.strip()
            st = identified_path_string[len(var):]
            # calculating case 2 and 3
            if st.startswith('['):
                end = re.search(']', st)
                if end:
                    path.predicate = st[1:end.start()]
                    path.path = self.parse_path(st[end.start()+1:])
            else:
                # case 1
                path.path = self.parse_path(st)
            return path
        else:
            raise ParsePathError("An error occured while parsing the path: "+identified_path_string)

    def parse_selection(self, sel):
        try:
            selection = Selection()
            top_result = re.match('TOP ', sel.upper())
            class_list = sel
            if top_result:
                top_split = sel.split(' ')
                top_number_string = top_split[1]
                top_number = int(top_number_string)
                selection.top = top_number
                top_number_lenght = len(top_number_string)
                class_list = sel[4+top_number_lenght:]
            try:
                classes = class_list.split(',')
                self.logger.debug("CLASSLIST: %s", class_list)
                for cl in classes:
                    variable = Variable()
                    class_tokens = cl.strip().split(" ")
                    if class_tokens and len(class_tokens) == 3:
                        variable.variable = self.parse_identified_path(class_tokens[0])
                        variable.label = class_tokens[2]
                    else:
                        variable.variable = self.parse_identified_path(cl)
                    selection.variables.append(variable)
            except Exception, ex:
                self.logger.error("ERROR: %s", ex)
                variable = Variable()
                class_tokens = class_list.strip().split(" ")
                variable.variable = self.parse_identified_path(class_list)
                if class_tokens and len(class_tokens) == 3:
                    variable.label = class_tokens[2]
                selection.variables.append(variable)
            return selection
        except Exception, e:
            self.logger.error("Error: %s", e)
            raise ParseSelectionError(str(e))

    # These functions are defined to parse the location part of an AQL statement
    def parse_class_expression(self, text):
        def is_openehr_variable(token):
            return 'openEHR-EHR' in token

        matching_obj = re.match('EHR |COMPOSITION |OBSERVATION ', text.upper())
        if matching_obj:
            class_expression = ClassExpression()
            end = matching_obj.end()
            class_expression.class_name = text[:end]
            optional_text = text[end:]
            tokens = optional_text.split()
            # Looking for the optional parts...
            # If it starts with [ it means is a predicate expression...
            if tokens[0].startswith('['):
                class_expression.predicate = self.parse_predicate(tokens[0].lstrip('[').rstrip(']'))
            else:
                # ... otherwise is a variable definition...
                pred = re.search('\[', tokens[0])
                if pred:
                    # ... followed by a predicate expression.
                    class_expression.variable_name = tokens[0][:pred.start()]
                    predicate = tokens[0][pred.start():]
                    if not is_openehr_variable(tokens[0]):
                        predicate = predicate.lstrip('[').rstrip(']')
                    class_expression.predicate = self.parse_predicate(predicate)
                else:
                    # ... without a predicate expression.
                    class_expression.variable_name = tokens[0]
                if len(tokens) > 1:
                    class_expression.predicate = self.parse_predicate(tokens[1].lstrip('[').rstrip(']'))
            return class_expression
        else:
            msg = "parse_class_expression ERROR. Expression: %s" % text
            self.logger.error(msg)
            raise ParsingError(msg)

    def parse_containers(self, text):
        conts = list(re.finditer('CONTAINS ', text.upper()))
        containers = []
        for i in xrange(len(conts)):
            c = conts[i]
            start = c.start()
            if i < len(conts)-1:
                end = conts[i+1].start()
                txt = text[9+start:end]
            else:
                txt = text[9+start:]
            class_expr = self.parse_class_expression(txt)
            container = Container()
            container.class_expression = class_expr
            containers.append(container)
        return containers

    def parse_location(self, location_string):
        """
        The FROM clause utilises class expressions and a set of containment criteria to specify the data source
        from which the query required data is to be retrieved.
        Its function is similar as the FROM clause of an SQL expression.
        """
        try:
            # A simple FROM clause consists of three parts: keyword - FROM,
            # class expression and/or containment constraints.
            #
            # Checking the keyword expression
            matching_obj = re.match('EHR |COMPOSITION |OBSERVATION ', location_string.upper())
            if matching_obj:
                location = Location()
                # Looking for containment expressions
                c = re.search(' CONTAINS ', location_string.upper())
                if c:
                    cpos = c.start()
                    # retrieving the containment expression
                    containment = location_string[cpos:]
                    location.containers = self.parse_containers(containment)
                    # retrieving the class expression
                    class_expr = location_string[:cpos]
                else:
                    # retrieving the class expression
                    class_expr = location_string
                location.class_expression = self.parse_class_expression(class_expr)
                return location
            else:
                msg = 'A class expression must have an openEHR RM class name, such as EHR, COMPOSITION, OBSERVATION'
                self.logger.error(msg)
                raise InvalidAQLError(msg)
        except Exception, e:
            msg = "An error occurred while parsing the location: " + str(e)
            self.logger.error(msg)
            raise ParseLocationError(msg)

    # Parse the condition part of an AQL statement
    def parse_condition(self, condition):
        try:
            cond = Condition()
            tokens = condition.split()
            if len(tokens) == 1:
                pred_expr = self.parse_predicate_expression(tokens[0])
                cond.condition = pred_expr
            else:
                cond_seq = ConditionSequence()
                for i, token in enumerate(tokens):
                    if token.upper() in ConditionOperator.LOGICAL_OPERATORS or \
                            token.upper() in ConditionOperator.ADVANCED_OPERATORS:
                        op = ConditionOperator()
                        op.op = token.strip().upper()
                        cond_seq.condition_sequence.append(op)
                    elif token.upper() in ConditionOperator.BASIC_OPERATORS:
                        pred_expr = self.parse_predicate_expression('%s %s %s' %
                                                                    (tokens[i - 1].strip(),
                                                                     token.strip(),
                                                                     tokens[i + 1].strip()))
                        cond_seq.condition_sequence.append(pred_expr)
                cond.condition = cond_seq
            return cond
        except Exception, e:
            raise ParseConditionError(e.message)

    # TBD...
    def parse_order_rules(self, order_rules):
        raise NotImplementedError()

    # TBD...
    def parse_time_constraints(self, time_constraints):
        raise NotImplementedError()