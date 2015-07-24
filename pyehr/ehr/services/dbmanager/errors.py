class DBManagerNotConnectedError(Exception):
    pass


class CascadeDeleteError(Exception):
    pass


class DuplicatedKeyError(Exception):
    pass


class InvalidRecordTypeError(Exception):
    pass


class InvalidJsonStructureError(Exception):
    pass


class UnknownDriverError(Exception):
    pass


class IndexServiceConnectionError(Exception):
    pass


class ConfigurationError(Exception):
    pass


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


class MissingOperatorError(Exception):
    pass


class MissingLocationExpressionError(Exception):
    pass


class InvalidFieldError(Exception):
    pass


class OptimisticLockError(Exception):
    pass


class RedundantUpdateError(Exception):
    pass


class RecordRestoreFailedError(Exception):
    pass


class RecordRestoreUnnecessaryError(Exception):
    pass


class MissingRevisionError(Exception):
    pass


class OperationNotAllowedError(Exception):
    pass


class QueryCreationException(Exception):
    pass
