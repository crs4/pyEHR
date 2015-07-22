from mongo_pm2 import MongoDriverPM2
import pymongo
import pymongo.errors
import time
from multiprocessing import Pool

from pyehr.ehr.services.dbmanager.querymanager.results_wrappers import ResultSet,\
    ResultColumnDef, ResultRow
from pyehr.ehr.services.dbmanager.errors import *

try:
    import simplejson as json
except ImportError:
    import json


class MultiprocessQueryRunnerPM3(object):

    def __init__(self, host, database, collection,
                 port, user, passwd):
        self.host = host
        self.database = database
        self.collection_name = collection
        self.port = port
        self.user = user
        self.passwd = passwd

    def __call__(self, query_description):
        driver_instance = MongoDriverPM3(
            self.host, self.database, self.collection_name,
            self.port, self.user, self.passwd
        )
        results = driver_instance._run_aql_query(
            query_description['condition'], query_description['selection'],
            query_description['aliases'], self.collection_name
        )
        return results

class MongoDriverPM3(MongoDriverPM2):
    """
    Create a driver to handle I\O with a MongoDB server. Using the given *host* and, if needed, *port*, *user*
    and *password* the driver will contact MongoDB when a connection is needed and will interrogate a specific
    *collection* stored in one *database* within the server. If no *logger* object is passed to constructor, a
    new one is created.
    """
    def connect(self):
        """
        Open a connection to a MongoDB server.
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
            self.logger.debug('Already connected to database %s, using collection %s',
                              self.database_name, self.collection_name)

    def disconnect(self):
        """
        Close a connection to a MongoDB server.
        """
        self.logger.debug('disconnecting from host %s', self.client.host)
        self.client.close()
        self.database = None
        self.collection = None
        self.client = None

    def add_record(self, record):
        """
        Save a record within MongoDB and return the record's ID

        :param record: the record that is going to be saved
        :type record: dictionary
        :return: the ID of the record
        """
        self._check_connection()
        try:
            return self.collection.insert_one(record).inserted_id
        except pymongo.errors.DuplicateKeyError:
            raise DuplicatedKeyError('A record with ID %s already exists' % record['_id'])

    def add_records(self, records, skip_existing_duplicated=False):
        """
        Save a list of records within MongoDB and return records' IDs. If skip_existing_duplicated is
        True, ignore duplicated key errors and continue with the save process, if it is False a
        DuplicatedKeyError will be raised and no record will be saved.

        :param records: the list of records that is going to be saved
        :type records: list
        :param skip_existing_duplicated: ignore duplicated key errors and save records with a unique ID
        :type skip_existing_duplicated: bool
        :return: a list of records' IDs and a list of records that caused a duplicated key error
        """
        # check for duplicated ID in records' batch
        if not len(records):
            return [],[]
        self._check_batch(records, '_id')
        self._check_connection()
        records_map = {r['_id']: r for r in records}
        duplicated_ids = [x['_id'] for x in self.get_records_by_query({'_id': {'$in': records_map.keys()}},
                                                                      {'_id': True})]
        if len(duplicated_ids) > 0 and not skip_existing_duplicated:
            raise DuplicatedKeyError('The following IDs are already in use: %s' % duplicated_ids)
        try:
            return self.collection.insert_many([x for k, x in records_map.iteritems()
                                                if k not in duplicated_ids]).inserted_ids,\
                [records_map[x] for x in duplicated_ids]
        except pymongo.errors.InvalidOperation:
            # empty bulk insert
            return [], [records_map[x] for x in duplicated_ids]


    def _update_record(self, record_id, update_condition):
        """
        Update an existing record

        :param record_id: record's ID
        :param update_condition: the update condition (in MongoDB syntax)
        :type update_condition: dictionary
        """
        self._check_connection()
        self.logger.debug('Updating record with ID %r, with condition %r', record_id,
                          update_condition)
        res = self.collection.update_one({'_id': record_id}, update_condition)
        self.logger.debug('updated %d documents', res.modified_count)

    def delete_record(self, record_id):
        """
        Delete an existing record

        :param record_id: record's ID
        """
        self._check_connection()
        self.logger.debug('deleting document with ID %s', record_id)
        res = self.collection.delete_one({'_id':record_id})
        self.logger.debug('deleted %d documents', res.deleted_count)

    def delete_records_by_query(self, query):
        """
        Delete all records that match the given query

        :param query: the query used to select records that will be deleted
        :type query: dict
        :return: the number of deleted records
        :rtype: int
        """
        self._check_connection()
        res = self.collection.delete_many(query)
        self.logger.debug('Deleted %d documents', res.deleted_count)
        return res.deleted_count

    def replace_record(self, record_id, new_record, update_timestamp_label=None):
        """
        Replace record with *record_id* with the given *new_record*

        :param record_id: the ID of the record that will be replaced with the new one
        :param new_record: the new record
        :type new_record: dict
        :param update_timestamp_label: the label of the *last_update* field of the record
                                       if the last update timestamp must be recorded or None
        :type update_timestamp_label: field label or None
        :return: the timestamp of the last update as saved in the DB or None
                 (if *update_timestamp_label* was None)
        """
        self._check_connection()
        last_update = None
        if update_timestamp_label:
            last_update = time.time()
            new_record[update_timestamp_label] = last_update
        try:
            new_record.pop('_id')
        except KeyError:
            pass
        self.collection.replace_one({"_id" : record_id}, new_record)
        return last_update

    def _find_by_aql_queries(self, queries, ehr_repository, query_processes):
        if len(queries) > 1:
            queries = self._aggregate_queries_by_selection(queries)
        total_results = ResultSet()
        if query_processes == 1 or len(queries) == 1:
            for query in queries:
                results = self._run_aql_query(query=query['condition'], fields=query['selection'],
                                              aliases=query['aliases'], collection=ehr_repository)
                total_results.extend(results)
        else:
            queries_pool = Pool(query_processes)
            results = queries_pool.imap_unordered(
                MultiprocessQueryRunnerPM3(self.host, self.database_name,
                                        ehr_repository, self.port, self.user, self.passwd),
                queries
            )
            for r in results:
                total_results.extend(r)
        return total_results

    def count_records_by_query(self, selector):
        """
        Retrieve the number of records matching the given query

        :param selector: the selector (in MongoDB syntax) used to select data
        :return: the number of records that match the given query
        :rtype: int
        """
        self._check_connection()
        return self.collection.count(selector)