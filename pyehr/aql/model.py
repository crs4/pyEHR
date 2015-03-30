from errors import OperatorNotSupported, ConditionNotSupported


class Primitive(object):
    def __init__(self):
        self.value = None


class PredicateExpression(object):
    def __init__(self):
        self.left_operand = None
        self.operand = None
        self.right_operand = None

    def __str__(self):
        str_list = ["PREDICATE_EXPRESSION"]
        if self.left_operand:
            str_list.append(" -> LEFT_OPERAND: %s" % str(self.left_operand))
        if self.operand:
            str_list.append(" -> OPERAND: %s" % str(self.operand))
        if self.right_operand:
            str_list.append(" -> RIGHT_OPERAND: %s" % str(self.right_operand))
        s = "\n".join(str_list)
        return s


class Predicate(object):
    def __init__(self):
        self.predicate_expression = None

    def __str__(self):
        str_list = ["PREDICATE"]
        if self.predicate_expression:
            str_list.append(" -> EXPRESSION: %s" % str(self.predicate_expression))
        s = "\n".join(str_list)
        return s


class NodePredicate(Predicate):
    def __init__(self):
        super(Predicate, self).__init__()
        self.archetype_id = None
        self.predicate_expression_list = []

    def __str__(self):
        str_list = ["NODE_PREDICATE"]
        if self.archetype_id:
            str_list.append(" -> ARCHETYPE_ID: %s" % str(self.archetype_id))
        for n in self.predicate_expression_list:
            str_list.append(" -> PREDICATE_EXPRESSION: %s" % str(n))
        s = "\n".join(str_list)
        return s


class ArchetypePredicate(Predicate):
    def __init__(self):
        super(ArchetypePredicate, self).__init__()
        self.archetype_id = None

    def __str__(self):
        str_list = ["ARCHETYPE_PREDICATE"]
        if self.archetype_id:
            str_list.append(" -> ARCHETYPE_ID: %s" % str(self.archetype_id))
        s = "\n".join(str_list)
        return s


class NodePath(object):
    def __init__(self):
        self.attribute_name = None
        self.predicate = None

    def __str__(self):
        str_list = ["PATH"]
        if self.attribute_name:
            str_list.append(" -> ATTRIBUTE_NAME: %s" % str(self.attribute_name))
        if self.attribute_name:
            str_list.append(" -> PREDICATE: %s" % str(self.predicate))
        s = "\n".join(str_list)
        return s


class Path(object):
    def __init__(self):
        super(Path, self).__init__()
        self.value = None
        self.separator = '/'
        self.node_list = []

    def __str__(self):
        str_list = ["PATH"]
        if self.value:
            str_list.append(" -> VALUE: %s" % str(self.value))
        for n in self.node_list:
            str_list.append(" -> NODE_PATH: %s" % str(n))
        s = "\n".join(str_list)
        return s


class IdentifiedPath(Primitive):
    def __init__(self):
        super(IdentifiedPath, self).__init__()
        self.separator = '/'
        self.predicate = None
        self.path = None
        self.variable = None

    def __str__(self):
        str_list = ["IDENTIFIED PATH"]
        if self.variable:
            str_list.append(" -> VARIABLE: %s" % str(self.variable))
        if self.predicate:
            str_list.append(" -> PREDICATE: %s" % str(self.predicate))
        if self.path:
            str_list.append(" -> PATH: %s" % str(self.path))
        s = "\n".join(str_list)
        return s


class LeafData(Primitive):
    def __init__(self):
        super(LeafData, self).__init__()


class Uri(Primitive):
    def __init__(self):
        super(Uri, self).__init__()


class ValueList(Primitive):
    def __init__(self):
        super(ValueList, self).__init__()


class ValueRange(Primitive):
    def __init__(self):
        super(ValueRange, self).__init__()


class Variable(object):
    def __init__(self):
        self.variable = None
        self.label = None

    def __str__(self):
        str_list = ["VARIABLE"]
        if self.variable:
            str_list.append(" -> VARIABLE: %s" % str(self.variable))
        if self.label:
            str_list.append(" -> LABEL: %s" % self.label)
        s = "\n".join(str_list)
        return s


class ClassExpression(object):
    def __init__(self):
        self.CLASS_TYPES = ("EHR", "COMPOSITION", "OBSERVATION")
        self._class_name = None
        self.variable_name = None
        self.predicate = None

    @property
    def class_name(self):
        return self._class_name

    @class_name.setter
    def class_name(self, value):
        self._class_name = value.strip()

    def __str__(self):
        str_list = ["CLASS_EXPRESSION"]
        if self.variable_name:
            str_list.append(" -> VARIABLE_NAME: %s" % self.variable_name)
        if self.class_name:
            str_list.append(" -> CLASS: %s" % self.class_name)
        if self.predicate:
            str_list.append(" -> PREDICATE: %s" % self.predicate)
        s = "\n".join(str_list)
        return s


class Container(object):
    def __init__(self):
        self.class_expression = None

    def __str__(self):
        str_list = ["CONTAINER"]
        str_list.append("CONTAINER")
        if self.class_expression:
            str_list.append(str(self.class_expression))
        s = "\n".join(str_list)
        return s


class Selection(object):
    def __init__(self):
        self.top = -1
        self.variables = []

    def __str__(self):
        str_list = ["SELECTION", " -> TOP: %d" % self.top]
        for v in self.variables:
            str_list.append(" -> VARIABLE: %s" % str(v))
        s = "\n".join(str_list)
        return s


class Location(object):
    def __init__(self):
        self.class_expression = None
        self.containers = []

    def get_variables_list(self):
        variable_list = []
        if self.class_expression:
            if self.class_expression.variable_name:
                variable_list.append(self.class_expression.variable_name)
        for cont in self.containers:
            if cont.class_expr.variableName:
                variable_list.append(cont.class_expr.variable_name)
        return variable_list

    def __str__(self):
        str_list = ["LOCATION"]
        if self.class_expression:
            str_list.append(" -> CLASS: %s" % self.class_expression)
        for c in self.containers:
            str_list.append(str(c))
        s = "\n".join(str_list)
        return s


class ConditionExpression(PredicateExpression):
    def __init__(self):
        super(ConditionExpression, self).__init__()
        self.expression = None

    def __str__(self):
        str_list = ["CONDITION_EXPRESSION"]
        if self.expression:
            str_list.append(" -> EXPRESSION: %s" % self.expression)
        s = "\n".join(str_list)
        return s


class ConditionOperator(object):

    BASIC_OPERATORS = (">", "<", ">=", "<=", "=", '!=')
    LOGICAL_OPERATORS = ('AND', 'OR')
    ADVANCED_OPERATORS = ('MATCHES', 'NOT', 'EXISTS')
    OPERATORS = BASIC_OPERATORS + LOGICAL_OPERATORS + ADVANCED_OPERATORS

    def __init__(self):
        self._op = None

    @property
    def op(self):
        return self._op

    @op.setter
    def op(self, value):
        if value.upper() in self.OPERATORS:
            self._op = value.upper()
        else:
            raise OperatorNotSupported("Unkown operator %s", value.upper())

    @op.deleter
    def op(self):
        self._op = None

    def __str__(self):
        str_list = ["CONDITION_OPERATOR"]
        if self._op:
            str_list.append(" -> OPERATOR: %s" % self._op)
        s = "\n".join(str_list)
        return s


class ConditionSequence(object):
    def __init__(self):
        # The conditionSequence can contain PredicateExpression, ConditionOperator and ConditionSequence instances
        self.condition_sequence = []


class Condition(object):
    def __init__(self):
        # The condition can be represented by a ConditionSequence or a ConditionExpression
        self._condition = None

    @property
    def condition(self):
        return self._condition

    @condition.setter
    def condition(self, value):
        if isinstance(value, ConditionSequence) or isinstance(value, PredicateExpression):
            self._condition = value
        else:
            raise ConditionNotSupported("Unknown condition %s", value)

    def __str__(self):
        str_list = ["CONDITION", str(self._condition)]
        s = "\n".join(str_list)
        return s


class OrderRules(object):
    def __init__(self):
        pass

    def __str__(self):
        return "None"


class TimeConstraints(object):
    def __init__(self):
        pass

    def __str__(self):
        return "None"


class QueryModel(object):
    def __init__(self):
        self.selection = None
        self.location = None
        self.condition = None
        self.order_rules = None
        self.time_constraints = None

    def __str__(self):
        str_list = list()
        str_list.append("-- SELECTION --")
        str_list.append(str(self.selection))
        str_list.append("-- LOCATION --")
        str_list.append(str(self.location))
        str_list.append("-- CONDITION --")
        str_list.append(str(self.condition))
        str_list.append("-- ORDER_RULES --")
        str_list.append(str(self.order_rules))
        str_list.append("-- TIME_CONSTRAINTS --")
        str_list.append(str(self.time_constraints))
        s = "\n".join(str_list)
        return s