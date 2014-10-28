from abc import ABCMeta, abstractmethod
from pyehr.ehr.services.dbmanager.errors import *
import re
from itertools import izip


class DriverInterface(object):
    """
    This abstract class acts as an interface for all the driver classes
    implemented to provide database services
    """
    __metaclass__ = ABCMeta

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.disconnect()
        return None

    @abstractmethod
    def connect(self):
        """
        Open a connection to the backend server
        """
        pass

    @abstractmethod
    def disconnect(self):
        """
        Close the connection to the backend server
        """
        pass

    @abstractmethod
    def init_structure(self, structure_def):
        """
        If needed, create a new data structure in the backend server defined
        by structure_def
        """
        pass

    @abstractmethod
    def encode_record(self, record):
        """
        Encode a :class:`Record` into a valid structure for the backend server
        """
        pass

    @abstractmethod
    def decode_record(self, record):
        """
        Encode a data structure coming from the backend server into a :class:`Record`
        object
        """
        pass

    @abstractmethod
    def add_record(self, record):
        """
        Add a record in the backend server
        """
        pass

    @abstractmethod
    def add_records(self, records, skip_existing_duplicated=False):
        """
        Add a list of records in the backed server
        """
        errors = list()
        saved = list()
        for r in records:
            try:
                saved.append(self.add_record(r))
            except DuplicatedKeyError, dke:
                if skip_existing_duplicated:
                    errors.append(r)
                else:
                    raise dke
        return saved, errors

    def _check_batch(self, records_batch, uid_field):
        """
        Check records batch for duplicated
        """
        from collections import Counter
        duplicated_counter = Counter()
        for r in records_batch:
            duplicated_counter[r[uid_field]] += 1
        if len(duplicated_counter) < len(records_batch):
            raise DuplicatedKeyError('The following IDs have one or more duplicated in this batch: %s' %
                                     [k for k, v in duplicated_counter.iteritems() if v > 1])

    @abstractmethod
    def get_record_by_id(self, record_id):
        """
        Retrieve a record by giving a record ID
        """
        pass

    @abstractmethod
    def get_record_by_version(self, record_id, version):
        """
        Retrieve a record by giving a record ID and a version number
        """
        pass

    @abstractmethod
    def get_all_records(self):
        """
        Retrieve all records from the backed server
        """
        pass

    @abstractmethod
    def delete_record(self, record_id):
        """
        Delete a record from the backend server by giving the record ID
        """
        pass

    @abstractmethod
    def delete_later_versions(self, record_id, version_to_keep=0):
        """
        Delete versions newer than version_to_keep for the given record ID
        """
        pass

    @abstractmethod
    def delete_records_by_query(self, query):
        """
        Delete all records that match the given query
        """
        pass

    @abstractmethod
    def update_field(self, record_id, field_label, field_value, update_timestamp_label,
                     increase_version):
        """
        Update the field with label *field_label* of the record with ID *record_id* with the
        value provided as *field_value* and update timestamp in field *update_timestamp_label*
        """
        pass

    @abstractmethod
    def replace_record(self, record_id, new_record, update_timestamp_label=None):
        """
        Replace record with *record_id* with the given *new_record*
        """
        pass

    @abstractmethod
    def add_to_list(self, record_id, list_label, item_value, update_timestamp_label,
                    increase_version):
        """
        Add the value provided with the *item_value* field to the list with label *list_label*
        of the record with ID *record_id* and update the timestamp in field *update_timestamp_label*
        """
        pass

    @abstractmethod
    def extend_list(self, record_id, list_label, items, update_timestamp_label,
                    increase_version):
        """
        Add values provided with the *items* field to the list with label *list_label*
        of the record with ID *record_id* and update the timestamp in field *update_timestamp_label*
        """
        update_timestamp = None
        for item in items:
            update_timestamp = self.add_to_list(record_id, list_label, item,
                                                update_timestamp_label)
        return update_timestamp

    @abstractmethod
    def remove_from_list(self, record_id, list_label, item_value, update_timestamp_label,
                         increase_version):
        """
        Remove the value provided with the *item_value* field from the list with label *list_label*
        of the record with ID *record_id* and update the timestamp in field *update_timestamp_label*
        """
        pass

    @abstractmethod
    def _map_operand(self, left, right, operand):
        pass

    @abstractmethod
    def _parse_expression(self, expression):
        pass

    @abstractmethod
    def _parse_simple_expression(self, expression):
        expr = {}
        operator = re.search('>=|>|<=|<|!=|=', expression)
        if operator:
            op1 = expression[0:operator.start()].strip('\'')
            op2 = expression[operator.end():].strip('\'')
            op = expression[operator.start():operator.end()].strip()
            try:
                expr.update(self._map_operand(op1, op2, op))
            except ValueError:
                msg = 'Invalid operator in expression %s' % expression
                self.logger.error(msg)
                raise ParseSimpleExpressionException(msg)
        else:
            raise MissingOperatorError('Missing operator in expression %s'
                                       % expression)
        return expr

    @abstractmethod
    def _parse_match_expression(self, expr):
        values = expr.expression.lstrip('{').rstrip('}').split(',')
        final = [v.strip('\'') for v in values]
        return final

    @abstractmethod
    def _normalize_path(self, path):
        pass

    @abstractmethod
    def _build_path(self, path):
        pass

    @abstractmethod
    def _build_paths(self, aliases):
        encoded_paths = dict()
        for var, details in aliases.iteritems():
            for path in details['paths']:
                p = self._build_path(path)
                encoded_paths.setdefault(var, []).append(p)
        return encoded_paths

    @abstractmethod
    def _extract_path_alias(self, path):
        return path.split('/')[0], '/'.join(path.split('/')[1:])

    @abstractmethod
    def _get_archetype_class_path(self, path):
        pass

    @abstractmethod
    def _calculate_condition_expression(self, condition, aliases):
        pass

    @abstractmethod
    def _compute_predicate(self, predicate):
        pass

    @abstractmethod
    def _calculate_ehr_expression(self, ehr_class_expression, query_params, patients_collection,
                                  ehr_collection):
        pass

    @abstractmethod
    def _calculate_location_expression(self, location, query_params, patients_collection,
                                       ehr_collection):
        pass

    @abstractmethod
    def _map_ehr_selection(self, path, ehr_var):
        pass

    @abstractmethod
    def _calculate_selection_expression(self, selection, aliases, ehr_alias):
        pass

    @abstractmethod
    def _split_results(self, query_results):
        pass

    @abstractmethod
    def _run_aql_query(self, query, fileds, aliases, collection):
        pass

    @abstractmethod
    def build_queries(self, query_model, patients_repository, ehr_repository, query_params=None):
        if not query_params:
            query_params = dict()
        selection = query_model.selection
        location = query_model.location
        condition = query_model.condition
        # TODO: add ORDER RULES and TIME CONSTRAINTS
        queries = []
        location_query, aliases, ehr_alias = self._calculate_location_expression(location, query_params,
                                                                                 patients_repository,
                                                                                 ehr_repository)
        if not location_query:
            return queries, []
        if condition:
            condition_results = self._calculate_condition_expression(condition, aliases)
            for condition_query, mappings in condition_results:
                selection_filter, results_aliases = self._calculate_selection_expression(selection, mappings,
                                                                                         ehr_alias)
                queries.append(
                    {
                        'query': (condition_query, selection_filter),
                        'results_aliases': results_aliases
                    }
                )
        else:
            paths = self._build_paths(aliases)
            for a in izip(*paths.values()):
                for p in [dict(izip(paths.keys(), a))]:
                    q = dict()
                    for k in aliases.keys():
                        q.update({self._get_archetype_class_path(p[k]): aliases[k]['archetype_class']})
                    selection_filter, results_aliases = self._calculate_selection_expression(selection, p,
                                                                                             ehr_alias)
                    queries.append(
                        {
                            'query': (q, selection_filter),
                            'results_aliases': results_aliases
                        }
                    )
        return queries, location_query

    @abstractmethod
    def execute_query(self, query_model, patients_repository, ehr_repository,
                      query_parameters):
        """
        Execute a query expressed as a :class:pyehr.aql.model.QueryModel` object
        """
        pass
