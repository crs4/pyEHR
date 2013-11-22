class Primitive(object):
    def __init__(self):
        self.value = None

class PredicateExpression(object):
    def __init__(self):
        self.leftOperand = None
        self.operand = None
        self.rightOperand = None

    def __str__(self):
        strList = []
        strList.append("")
        strList.append("PREDICATE_EXPRESSION")
        if self.leftOperand:
            strList.append(" -> LEFT_OPERAND: %s" % str(self.leftOperand))
        if self.operand:
            strList.append(" -> OPERAND: %s" % str(self.operand))
        if self.rightOperand:
            strList.append(" -> RIGHT_OPERAND: %s" % str(self.rightOperand))
        s = "\n".join(strList)
        return s

class  Predicate(object):
    def __init__(self):
        self.predicateExpression = None

    def __str__(self):
        strList = []
        strList.append("")
        strList.append("PREDICATE")
        if self.predicateExpression:
            strList.append(" -> EXPRESSION: %s" % str(self.predicateExpression))
        s = "\n".join(strList)
        return s

class  NodePredicate(Predicate):
    def __init__(self):
        self.archetypeId = None
        self.predicateExpressionList = []

    def __str__(self):
        strList = []
        strList.append("")
        strList.append("NODE_PREDICATE")
        if self.archetypeId:
            strList.append(" -> ARCHETYPE_ID: %s" % str(self.archetypeId))
        for n in self.predicateExpressionList:
            strList.append(" -> PREDICATE_EXPRESSION: %s" % str(n))
        s = "\n".join(strList)
        return s

class ArchetypePredicate(Predicate):
    def __init__(self):
        self.archetypeId = None

    def __str__(self):
        strList = []
        strList.append("")
        strList.append("ARCHETYPE_PREDICATE")
        if self.archetypeId:
            strList.append(" -> ARCHETYPE_ID: %s" % str(self.archetypeId))
        s = "\n".join(strList)
        return s

class NodePath(object):
    def __init__(self):
        self.attributeName = None
        self.predicate = None

    def __str__(self):
        strList = []
        strList.append("")
        strList.append("PATH")
        if self.attributeName:
            strList.append(" -> ATTRIBUTE_NAME: %s" % str(self.attributeName))
        if self.attributeName:
            strList.append(" -> PREDICATE: %s" % str(self.predicate))
        s = "\n".join(strList)
        return s

class Path(object):
    def __init__(self):
        super(Path, self).__init__()
        self.value = None
        self.separator = '/'
        self.nodeList = []

    def __str__(self):
        strList = []
        strList.append("")
        strList.append("PATH")
        if self.value:
            strList.append(" -> VALUE: %s" % str(self.value))
        for n in self.nodeList:
            strList.append(" -> NODE_PATH: %s" % str(n))
        s = "\n".join(strList)
        return s

class IdentifiedPath(Primitive):
    def __init__(self):
        super(IdentifiedPath, self).__init__()
        self.separator = '/'
        self.predicate = None
        self.path = None
        self.variable = None

    def __str__(self):
        strList = []
        strList.append("")
        strList.append("- IDENTIFIEDPATH -")
        if self.variable:
            strList.append(" -> VARIABLE: %s" % str(self.variable))
        if self.predicate:
            strList.append(" -> PREDICATE: %s" % str(self.predicate))
        if self.path:
            strList.append(" -> PATH: %s" % str(self.path))
        s = "\n".join(strList)
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

class Variable():
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
        strList = []
        strList.append("")
        strList.append("VARIABLE")
        if self.variable:
            strList.append(" -> VARIABLE: %s" % str(self.variable))
        if self.label:
            strList.append(" -> LABEL: %s" % self.label)
        s = "\n".join(strList)
        return s

class ClassExpression():
    def __init__(self):
        self.CLASS_TYPES = ("EHR", "COMPOSITION", "OBSERVATION")
        self.className = None
        self.variableName = None
        self.predicate = None

    def _print_(self):
        print ""
        print "CLASS_EXPRESSION"
        if self.className:
            print " -> CLASS_NAME: %s" % self.className
        if self.variableName:
            print " -> VARIABLE_NAME: %s" % self.variableName
        if self.predicate:
            print " -> PREDICATE: %s" % self.predicate

    def __str__(self):
        strList = []
        strList.append("")
        strList.append("CLASS_EXPRESSION")
        if self.variableName:
            strList.append(" -> VARIABLE_NAME: %s" % self.variableName)
        if self.className:
            strList.append(" -> CLASS: %s" % self.className)
        if self.predicate:
            strList.append(" -> PREDICATE: %s" % self.predicate)
        s = "\n".join(strList)
        return s

class Container():
    def __init__(self):
        self.classExpr = None

    def _print_(self):
        print "-------------------------------------------"
        print "CONTAINER"
        if self.classExpr:
            self.classExpr._print_()
        print "-------------------------------------------"

    def __str__(self):
        strList = []
        strList.append("-------------------------------------------")
        strList.append("CONTAINER")
        if self.classExpr:
            strList.append(str(self.classExpr))
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
        self.classExpression = None
        self.containers = []

    def getVariableList(self):
        variableList = []
        if self.classExpression:
            if self.classExpression.variableName:
                variableList.append(self.classExpression.variableName)
        for cont in self.containers:
            if cont.classExpr.variableName:
                variableList.append(cont.classExpr.variableName)
        return variableList

    def _print_(self):
        print ""
        print "LOCATION"
        if self.classExpression:
            print " -> CLASS: %s" % self.classExpression
        for c in self.containers:
            c._print_()

    def __str__(self):
        strList = []
        strList.append("")
        strList.append("LOCATION")
        if self.classExpression:
            strList.append(" -> CLASS: %s" % self.classExpression)
        for c in self.containers:
            strList.append(str(c))
        s = "\n".join(strList)
        return s

class ConditionExpression(object):
    def __init__(self):
        self.expression = None
    
    def _print_(self):
        print ""
        print "CONDITION_EXPRESSION"
        if self.expression:
            print " -> EXPRESSION: %s" % self.expression

    def __str__(self):
        strList = []
        strList.append("")
        strList.append("CONDITION_EXPRESSION")
        if self.expression:
            strList.append(" -> EXPRESSION: %s" % self.expression)
        s = "\n".join(strList)
        return s

class OperatorNotSupported(Exception):
    def __init__(self):
        pass

class ConditionNotSupported(Exception):
    def __init__(self):
        pass

class ConditionOperator(object):
    def __init__(self):
        self.typeKeywords = ("OR", "AND", "MATCHES", ">", "<", ">=", "<=", "=")
        self._op = None

    @property
    def op(self):
        return self._op

    @op.setter
    def op(self, value):
        print("OP %r" % value)
        if value.upper() in self.typeKeywords:
            self._op = value.upper()
        else:
            raise OperatorNotSupported()

    @op.deleter
    def op(self):
        del self._op

    def _print_(self):
        print ""
        print "CONDITION_OPERATOR"
        if self._op:
            print "OPERATOR: %s" % self._op

    def __str__(self):
        strList = []
        strList.append("")
        strList.append("CONDITION_OPERATOR")
        if self._op:
            strList.append(" -> OPERATOR: %s" % self._op)
        s = "\n".join(strList)
        return s

class ConditionSequence(object):
    def __init__(self):
        # The conditionSequence can contain ConditionExpression, ConditionOperator and ConditionSequence instances
        self.conditionSequence = []

class Condition(object):
    def __init__(self):
        # The condition can be represented by a ConditionSequence or a ConditionExpression
        self._condition = None

    @property
    def condition(self):
        return self._condition

    @condition.setter
    def condition(self, value):
        if isinstance(value,ConditionSequence) or isinstance(value,ConditionExpression):
            self._op = value
        else:
            raise ConditionNotSupported()

    def __str__(self):
        strList = []
        strList.append("")
        strList.append("CONDITION")
        strList.append("")
        strList.append(str(self._condition))
        s = "\n".join(strList)
        return s

class OrderRules():
    def __init__(self):
        pass

    def _print_(self):
        pass

    def __str__(self):
        return "None"

class TimeConstraints():
    def __init__(self):
        pass

    def _print_(self):
        pass

    def __str__(self):
        return "None"

class QueryModel():
    def __init__(self):
        self.selection = None
        self.location = None
        self.condition = None
        self.orderRules = None
        self.timeConstraints = None

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
        self.orderRules._print_()
        print "========================================================================="
        print "TIME_CONSTRAINTS"
        print "-------------------------------------------------------------------------"
        self.timeConstraints._print_()
        print "========================================================================="

    def __str__(self):
        strList = []
        strList.append("=========================================================================")
        strList.append("SELECTION")
        strList.append("-------------------------------------------------------------------------")
        strList.append(str(self.selection))
        strList.append("=========================================================================")
        strList.append("LOCATION")
        strList.append("-------------------------------------------------------------------------")
        strList.append(str(self.location))
        strList.append("=========================================================================")
        strList.append("CONDITION")
        strList.append("-------------------------------------------------------------------------")
        strList.append(str(self.condition))
        strList.append("=========================================================================")
        strList.append("ORDER_RULES")
        strList.append("-------------------------------------------------------------------------")
        strList.append(str(self.orderRules))
        strList.append("=========================================================================")
        strList.append("TIME_CONSTRAINTS")
        strList.append("-------------------------------------------------------------------------")
        strList.append(str(self.timeConstraints))
        strList.append("=========================================================================")
        s = "\n".join(strList)
        return s