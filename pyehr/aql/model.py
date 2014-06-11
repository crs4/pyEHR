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
        str_list = []
        str_list.append("")
        str_list.append("PREDICATE_EXPRESSION")
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
        str_list = []
        str_list.append("")
        str_list.append("PREDICATE")
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
        str_list = []
        str_list.append("")
        str_list.append("NODE_PREDICATE")
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
        str_list = []
        str_list.append("")
        str_list.append("ARCHETYPE_PREDICATE")
        if self.archetype_id:
            str_list.append(" -> ARCHETYPE_ID: %s" % str(self.archetype_id))
        s = "\n".join(str_list)
        return s


class NodePath(object):
    def __init__(self):
        self.attribute_name = None
        self.predicate = None

    def __str__(self):
        str_list = []
        str_list.append("")
        str_list.append("PATH")
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
        str_list = []
        str_list.append("")
        str_list.append("PATH")
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
        str_list = []
        str_list.append("")
        str_list.append("- IDENTIFIEDPATH -")
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

    def _print_(self):
        print ""
        print "VARIABLE"
        if self.variable:
            print " -> VARIABLE: %s" % str(self.variable)
        if self.label:
            print " -> LABEL: %s" % self.label

    def __str__(self):
        str_list = []
        str_list.append("")
        str_list.append("VARIABLE")
        if self.variable:
            str_list.append(" -> VARIABLE: %s" % str(self.variable))
        if self.label:
            str_list.append(" -> LABEL: %s" % self.label)
        s = "\n".join(str_list)
        return s


class ClassExpression(object):
    def __init__(self):
        self.CLASS_TYPES = ("EHR", "COMPOSITION", "OBSERVATION")
        self.class_name = None
        self.variable_name = None
        self.predicate = None

    def _print_(self):
        print ""
        print "CLASS_EXPRESSION"
        if self.class_name:
            print " -> CLASS_NAME: %s" % self.class_name
        if self.variable_name:
            print " -> VARIABLE_NAME: %s" % self.variable_name
        if self.predicate:
            print " -> PREDICATE: %s" % self.predicate

    def __str__(self):
        strList = []
        strList.append("")
        strList.append("CLASS_EXPRESSION")
        if self.variable_name:
            strList.append(" -> VARIABLE_NAME: %s" % self.variable_name)
        if self.class_name:
            strList.append(" -> CLASS: %s" % self.class_name)
        if self.predicate:
            strList.append(" -> PREDICATE: %s" % self.predicate)
        s = "\n".join(strList)
        return s


class Container(object):
    def __init__(self):
        self.class_expr = None

    def _print_(self):
        print "-------------------------------------------"
        print "CONTAINER"
        if self.class_expr:
            self.class_expr._print_()
        print "-------------------------------------------"

    def __str__(self):
        strList = []
        strList.append("-------------------------------------------")
        strList.append("CONTAINER")
        if self.class_expr:
            strList.append(str(self.class_expr))
        strList.append("-------------------------------------------")
        s = "\n".join(strList)
        return s


class Selection(object):
    def __init__(self):
        self.top = -1
        self.variables = []

    def _print_(self):
        print "SELECTION"
        print " -> TOP: %d" % self.top
        for v in self.variables:
            print " -> VARIABLE: %s" % v._print_()

    def __str__(self):
        strList = []
        strList.append("")
        strList.append("SELECTION")
        strList.append(" -> TOP: %d" % self.top)
        for v in self.variables:
           strList.append(" -> VARIABLE: %s" % str(v))
        s = "\n".join(strList)
        return s


class Location(object):
    def __init__(self):
        self.class_expression = None
        self.containers = []

    def getVariableList(self):
        variableList = []
        if self.class_expression:
            if self.class_expression.variableName:
                variableList.append(self.class_expression.variableName)
        for cont in self.containers:
            if cont.classExpr.variableName:
                variableList.append(cont.classExpr.variableName)
        return variableList

    def _print_(self):
        print ""
        print "LOCATION"
        if self.class_expression:
            print " -> CLASS: %s" % self.class_expression
        for c in self.containers:
            c._print_()

    def __str__(self):
        strList = []
        strList.append("")
        strList.append("LOCATION")
        if self.class_expression:
            strList.append(" -> CLASS: %s" % self.class_expression)
        for c in self.containers:
            strList.append(str(c))
        s = "\n".join(strList)
        return s


# class ConditionExpression(PredicateExpression):
#     def __init__(self):
#         self.expression = None
#
#     def _print_(self):
#         print ""
#         print "CONDITION_EXPRESSION"
#         if self.expression:
#             print " -> EXPRESSION: %s" % self.expression
#
#     def __str__(self):
#         strList = []
#         strList.append("")
#         strList.append("CONDITION_EXPRESSION")
#         if self.expression:
#             strList.append(" -> EXPRESSION: %s" % self.expression)
#         s = "\n".join(strList)
#         return s


class ConditionOperator(object):
    def __init__(self):
        self.type_keywords = ("OR", "AND", "MATCHES", ">", "<", ">=", "<=", "=")
        self._op = None

    @property
    def op(self):
        return self._op

    @op.setter
    def op(self, value):
        if value.upper() in self.type_keywords:
            self._op = value.upper()
        else:
            raise OperatorNotSupported("Unkown operator %s", value.upper())

    @op.deleter
    def op(self):
        self._op = None

    def _print_(self):
        print ""
        print "CONDITION_OPERATOR"
        if self._op:
            print "OPERATOR: %s" % self._op

    def __str__(self):
        str_list = []
        str_list.append("")
        str_list.append("CONDITION_OPERATOR")
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
        str_list = []
        str_list.append("")
        str_list.append("CONDITION")
        str_list.append("")
        str_list.append(str(self._condition))
        s = "\n".join(str_list)
        return s


class OrderRules(object):
    def __init__(self):
        pass

    def _print_(self):
        pass

    def __str__(self):
        return "None"


class TimeConstraints(object):
    def __init__(self):
        pass

    def _print_(self):
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

    def _print_(self):
        print "========================================================================="
        print "SELECTION"
        print "-------------------------------------------------------------------------"
        self.selection._print_()
        print "========================================================================="
        print "LOCATION"
        print "-------------------------------------------------------------------------"
        self.location._print_()
        print "========================================================================="
        print "CONDITION"
        print "-------------------------------------------------------------------------"
        self.condition._print_()
        print "========================================================================="
        print "ORDER_RULES"
        print "-------------------------------------------------------------------------"
        self.order_rules._print_()
        print "========================================================================="
        print "TIME_CONSTRAINTS"
        print "-------------------------------------------------------------------------"
        self.time_constraints._print_()
        print "========================================================================="

    def __str__(self):
        str_list = []
        str_list.append("=========================================================================")
        str_list.append("SELECTION")
        str_list.append("-------------------------------------------------------------------------")
        str_list.append(str(self.selection))
        str_list.append("=========================================================================")
        str_list.append("LOCATION")
        str_list.append("-------------------------------------------------------------------------")
        str_list.append(str(self.location))
        str_list.append("=========================================================================")
        str_list.append("CONDITION")
        str_list.append("-------------------------------------------------------------------------")
        str_list.append(str(self.condition))
        str_list.append("=========================================================================")
        str_list.append("ORDER_RULES")
        str_list.append("-------------------------------------------------------------------------")
        str_list.append(str(self.order_rules))
        str_list.append("=========================================================================")
        str_list.append("TIME_CONSTRAINTS")
        str_list.append("-------------------------------------------------------------------------")
        str_list.append(str(self.time_constraints))
        str_list.append("=========================================================================")
        s = "\n".join(str_list)
        return s