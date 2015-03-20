import time, sys, argparse, json, os, multiprocessing
from random import randint
from functools import wraps
from pyehr.ehr.services.dbmanager.dbservices.wrappers import ClinicalRecord, PatientRecord
from pyehr.ehr.services.dbmanager.dbservices import DBServices
from pyehr.ehr.services.dbmanager.querymanager import QueryManager
from pyehr.ehr.services.dbmanager.querymanager.queries_runner import QueriesRunner
from pyehr.utils.services import get_service_configuration, get_logger

from structures_builder import build_record, contains_archetype


class DataLoaderThread(multiprocessing.Process):
    def __init__(self, db_service_conf, index_service_conf,
                 patients_size, ehrs_size, thread_index, threads_size,
                 archetypes_dir, record_structures, logger, matching_instances, start_patient_id):
        multiprocessing.Process.__init__(self)
        self.db_service = DBServices(**db_service_conf)
        self.db_service.set_index_service(**index_service_conf)
        self.patients_size = patients_size
        self.ehrs_size = ehrs_size
        self.thread_index = thread_index
        self.threads_size = threads_size
        self.record_structures = record_structures
        self.archetypes_dir = archetypes_dir
        self.logger = logger
        self.matching_counter = matching_instances
        self.start_patient_id = start_patient_id

    def run(self):
        self.logger.debug('RUNNING DATASET BUILD THREAD %d of %d', self.thread_index+1,
                          self.threads_size)
        selected_structures = dict()
        for path in [
            ['composition.lbl-00001', 'composition.lbl-00017', 'composition.lbl-00013', 'blood_pressure'],
            ['composition.lbl-00009', 'composition.lbl-00008', 'composition.lbl-00007', 'urin_analysis']
        ]:
            for structure in self.record_structures:
                matched, label = contains_archetype(structure, path)
                if matched:
                    selected_structures.setdefault(label, []).append(structure)
        if len(selected_structures['blood_pressure']) == 0 or len(selected_structures['urin_analysis']) == 0:
            raise ValueError('Not enough structures were created to match hits')
        self.logger.debug('Blood Pressure structures are %d' % len(selected_structures['blood_pressure']))
        self.logger.debug('Urine analysis structures are %d' % len(selected_structures['urin_analysis']))
        for x in xrange(self.start_patient_id, self.start_patient_id + self.patients_size):
            crecs = list()
            if x % self.threads_size == self.thread_index:
                p = self.db_service.get_patient('PATIENT_%05d' % x, fetch_ehr_records=False)
                create_match = False
                if p is None:
                    p = self.db_service.save_patient(PatientRecord('PATIENT_%05d' % x))
                    self.logger.debug('Saved patient PATIENT_%05d', x)
                    create_match = True
                else:
                    self.logger.debug('Patient PATIENT_%05d already exists, using it', x)
                    self.matching_counter['intersect'] -= 1
                    self.matching_counter['blood_pressure'] -= 1
                    self.matching_counter['urin_analysis'] -= 1
                num_rec = self.ehrs_size - len(p.ehr_records)
                if self.matching_counter['intersect'] > 0 and create_match:
                    self.matching_counter['blood_pressure'] -= 1
                    st = selected_structures['blood_pressure'][randint(0, len(selected_structures['blood_pressure']) - 1)]
                    arch = build_record(st, self.archetypes_dir, True, 'blood_pressure')
                    crecs.append(ClinicalRecord(arch))
                    self.matching_counter['urin_analysis'] -= 1
                    st = selected_structures['urin_analysis'][randint(0, len(selected_structures['urin_analysis']) - 1)]
                    arch = build_record(st, self.archetypes_dir, True, 'urin_analysis')
                    crecs.append(ClinicalRecord(arch))
                    self.matching_counter['intersect'] -= 1
                    num_rec -= 2
                    create_match = False
                if self.matching_counter['blood_pressure'] > 0 and create_match:
                    self.matching_counter['blood_pressure'] -= 1
                    st = selected_structures['blood_pressure'][randint(0, len(selected_structures['blood_pressure']) - 1)]
                    arch = build_record(st, self.archetypes_dir, True, 'blood_pressure')
                    crecs.append(ClinicalRecord(arch))
                    num_rec -= 1
                    create_match = False
                if self.matching_counter['urin_analysis'] > 0 and create_match:
                    self.matching_counter['urin_analysis'] -= 1
                    st = selected_structures['urin_analysis'][randint(0, len(selected_structures['urin_analysis']) - 1)]
                    arch = build_record(st, self.archetypes_dir, True, 'urin_analysis')
                    crecs.append(ClinicalRecord(arch))
                    num_rec -= 1
                for i in xrange(num_rec):
                    st = self.record_structures[randint(0, len(self.record_structures) - 1)]
                    arch = build_record(st, self.archetypes_dir, False)
                    crecs.append(ClinicalRecord(arch))
                self.logger.debug('Done building %d EHRs', (self.ehrs_size - len(p.ehr_records)))
                for chunk in self.records_by_chunk(crecs):
                    self.db_service.save_ehr_records(chunk, p)
                self.logger.debug('EHRs saved')

    def records_by_chunk(self, records, batch_size=500):
        offset = 0
        while(len(records[offset:])) > 0:
            yield records[offset:offset+batch_size]
            offset += batch_size


class QueryPerformanceTest(object):

    def __init__(self, pyehr_conf_file, archetypes_dir, structures_description_file,
                 matching_instances, start_patient_id, log_file=None, log_level='INFO', db_name_prefix=None):
        self.logger = get_logger('query_performance_test',
                         log_file=log_file, log_level=log_level)
        sconf = get_service_configuration(pyehr_conf_file)
        self.db_conf = sconf.get_db_configuration()
        self.index_conf = sconf.get_index_configuration()
        if db_name_prefix:
            self.db_conf['database'] = '%s_%s' % (self.db_conf['database'], db_name_prefix)
            self.index_conf['database'] = '%s_%s' % (self.index_conf['database'], db_name_prefix)
        self.db_service = DBServices(**self.db_conf)
        self.db_service.set_index_service(**self.index_conf)
        self.query_manager = QueryManager(**self.db_conf)
        self.query_manager.set_index_service(**self.index_conf)
        self.query_manager.index_service.logger = self.logger
        self.queries_runner = QueriesRunner(self.db_conf, self.index_conf)
        self.archetypes_dir = archetypes_dir
        self.structures_file = structures_description_file
        self.matching_instances = matching_instances
        self.start_patient_id = start_patient_id
        self.setup_sharding()

    def setup_sharding(self):
        def build_shard_collection_command(db, coll, shard_keys):
            return "sh.shardCollection('%s.%s', %r)" % (db, coll, shard_keys)

        self.logger.info('Enabling sharding for database %s', self.db_service.database)
        # Automatically setup MongoDB sharding for used database
        base_command = "mongo %s --eval" % self.db_service.host
        add_to_shard_command = "%s \"%s\"" % (base_command, "sh.enableSharding('%s')" % self.db_service.database)
        shard_patients_coll = "%s \"%s\"" % (base_command, build_shard_collection_command(self.db_service.database,
                                                                                          self.db_service.patients_repository,
                                                                                          {"_id": "hashed"}))
        shard_ehrs_coll = "%s \"%s\"" % (base_command, build_shard_collection_command(self.db_service.database,
                                                                                      self.db_service.ehr_repository,
                                                                                      {'ehr_structure_id': 1, 'patient_id': 1}))
        for cmd in [add_to_shard_command, shard_patients_coll, shard_ehrs_coll]:
            self.logger.debug(cmd)
            os.system(cmd)
        self.logger.info('Sharding configured')

    def get_execution_time(f):
        @wraps(f)
        def wrapper(inst, *args, **kwargs):
            start_time = time.time()
            res = f(inst, *args, **kwargs)
            execution_time = time.time() - start_time
            inst.logger.info('Execution of \'%s\' took %f seconds' % (f.func_name, execution_time))
            return res, execution_time
        return wrapper

    @get_execution_time
    def build_dataset(self, patients, ehrs, threads):
        def _get_instances_in_thread(instances, threads):
            if threads > instances:
                return [instances]
            mod = instances % threads
            if mod == 0:
                return [instances/threads for i in xrange(0, threads)]
            else:
                parts = [instances/threads for i in xrange(0, threads)]
                for x in xrange(0, mod):
                    parts[x] += 1
                return parts

        with open(self.structures_file) as f:
            structures = json.loads(f.read())
        build_threads = []
        bp_parts = _get_instances_in_thread(self.matching_instances['blood_pressure'], threads)
        ua_parts = _get_instances_in_thread(self.matching_instances['urin_analysis'], threads)
        i_parts = _get_instances_in_thread(self.matching_instances['intersect'], threads)
        for i, x in enumerate(xrange(threads)):
            try:
                matching_count = {
                    'blood_pressure': bp_parts[i],
                    'urin_analysis': ua_parts[i],
                    'intersect': i_parts[i]
                }
            except IndexError:
                matching_count = {
                    'blood_pressure': 0,
                    'urin_analysis': 0,
                    'intersect': 0
                }
            t = DataLoaderThread(self.db_conf, self.index_conf, patients, ehrs, x, threads,
                                 self.archetypes_dir, structures, self.logger,
                                 matching_count, self.start_patient_id)
            build_threads.append(t)
        for t in build_threads:
            t.start()
        for t in build_threads:
            t.join()
        drf = self.db_service._get_drivers_factory(self.db_service.ehr_repository)
        with drf.get_driver() as driver:
            self.logger.info('*** Produced %d different structures ***',
                             len(driver.collection.distinct('ehr_structure_id')))

    def execute_query(self, query, params=None):
        results = self.query_manager.execute_aql_query(query, params)
        self.logger.info('Retrieved %d records' % results.total_results)
        return results

    def execute_queries_intersection(self, queries, intersection_field_label):
        for label, query in queries.iteritems():
            self.queries_runner.add_query(label, query)
        self.queries_runner.execute_queries()
        results = self.queries_runner.get_intersection(intersection_field_label, *queries.keys())
        self.logger.info('Retrieved %d records' % len(results))
        return results

    @get_execution_time
    def execute_select_all_query(self):
        query = """
        SELECT e/ehr_id/value AS patient_identifier
        FROM Ehr e
        CONTAINS Observation o[openEHR-EHR-OBSERVATION.blood_pressure.v1]
        """
        return self.execute_query(query)

    @get_execution_time
    def execute_select_all_patient_query(self, patient_index=0):
        query = """
        SELECT o/data[at0001]/events[at0006]/data[at0003]/items[at0004]/value/magnitude AS systolic,
        o/data[at0001]/events[at0006]/data[at0003]/items[at0005]/value/magnitude
        FROM Ehr e [uid=$ehrUid]
        CONTAINS Observation o[openEHR-EHR-OBSERVATION.blood_pressure.v1]
        """
        return self.execute_query(query, {'ehrUid': 'PATIENT_%05d' % patient_index})

    @get_execution_time
    def execute_filtered_query(self):
        query = """
        SELECT o/data[at0001]/events[at0006]/data[at0003]/items[at0004]/value/magnitude as systolic,
        o/data[at0001]/events[at0006]/data[at0003]/items[at0005]/value/magnitude as dyastolic
        FROM Ehr e
        CONTAINS Observation o[openEHR-EHR-OBSERVATION.blood_pressure.v1]
        WHERE o/data[at0001]/events[at0006]/data[at0003]/items[at0004]/value/magnitude >= 121
        OR o/data[at0001]/events[at0006]/data[at0003]/items[at0005]/value/magnitude >= 80
        """
        return self.execute_query(query)

    @get_execution_time
    def execute_patient_filtered_query(self, patient_index=0):
        query = """
        SELECT o/data[at0001]/events[at0006]/data[at0003]/items[at0004]/value/magnitude AS systolic,
        o/data[at0001]/events[at0006]/data[at0003]/items[at0005]/value/magnitude
        FROM Ehr e [uid=$ehrUid]
        CONTAINS Observation o[openEHR-EHR-OBSERVATION.blood_pressure.v1]
        WHERE o/data[at0001]/events[at0006]/data[at0003]/items[at0004]/value/magnitude >= 180
        OR o/data[at0001]/events[at0006]/data[at0003]/items[at0005]/value/magnitude >= 110
        """
        return self.execute_query(query, {'ehrUid': 'PATIENT_%05d' % patient_index})

    @get_execution_time
    def blood_pressure_execute_patient_count_query(self):
        query = """
        SELECT e/ehr_id/value AS patient_identifier
        FROM Ehr e
        CONTAINS Composition c0[openEHR-EHR-COMPOSITION.encounter.v1.lbl-00001]
        CONTAINS Composition c1[openEHR-EHR-COMPOSITION.encounter.v1.lbl-00017]
        CONTAINS Composition c2[openEHR-EHR-COMPOSITION.encounter.v1.lbl-00013]
        CONTAINS Observation o[openEHR-EHR-OBSERVATION.blood_pressure.v1]
        WHERE o/data[at0001]/events[at0006]/data[at0003]/items[at0004]/value/magnitude >= 121
        AND o/data[at0001]/events[at0006]/data[at0003]/items[at0005]/value/magnitude >= 80
        """
        return self.execute_query(query)

    @get_execution_time
    def urine_analysis_execute_patient_count_query(self):
        query = """
        SELECT e/ehr_id/value AS patient_identifier
        FROM Ehr e
        CONTAINS Composition c0[openEHR-EHR-COMPOSITION.encounter.v1.lbl-00009]
        CONTAINS Composition c1[openEHR-EHR-COMPOSITION.encounter.v1.lbl-00008]
        CONTAINS Composition c3[openEHR-EHR-COMPOSITION.encounter.v1.lbl-00007]
        CONTAINS Observation o[openEHR-EHR-OBSERVATION.urinalysis.v1]
        WHERE o/data[at0001]/events[at0002]/data[at0003]/items[at0050]/value = at0120
        AND o/data[at0001]/events[at0002]/data[at0003]/items[at0095]/value = at0101
        """
        return self.execute_query(query)

    @get_execution_time
    def blood_pressure_urine_analysis_intersection(self):
        bp_query = """
        SELECT e/ehr_id/value AS patient_identifier
        FROM Ehr e
        CONTAINS Composition c0[openEHR-EHR-COMPOSITION.encounter.v1.lbl-00001]
        CONTAINS Composition c1[openEHR-EHR-COMPOSITION.encounter.v1.lbl-00017]
        CONTAINS Composition c2[openEHR-EHR-COMPOSITION.encounter.v1.lbl-00013]
        CONTAINS Observation o[openEHR-EHR-OBSERVATION.blood_pressure.v1]
        WHERE o/data[at0001]/events[at0006]/data[at0003]/items[at0004]/value/magnitude >= 121
        AND o/data[at0001]/events[at0006]/data[at0003]/items[at0005]/value/magnitude >= 80
        """
        ua_query = """
        SELECT e/ehr_id/value AS patient_identifier
        FROM Ehr e
        CONTAINS Composition c0[openEHR-EHR-COMPOSITION.encounter.v1.lbl-00009]
        CONTAINS Composition c1[openEHR-EHR-COMPOSITION.encounter.v1.lbl-00008]
        CONTAINS Composition c3[openEHR-EHR-COMPOSITION.encounter.v1.lbl-00007]
        CONTAINS Observation o[openEHR-EHR-OBSERVATION.urinalysis.v1]
        WHERE o/data[at0001]/events[at0002]/data[at0003]/items[at0050]/value = at0120
        AND o/data[at0001]/events[at0002]/data[at0003]/items[at0095]/value = at0101
        """
        queries = {
            'blood_pressure': bp_query,
            'urine_analysis': ua_query
        }
        return self.execute_queries_intersection(queries, 'patient_identifier')

    def cleanup(self):
        drf = self.db_service._get_drivers_factory(self.db_service.ehr_repository)
        with drf.get_driver() as driver:
            driver.collection.remove()
            driver.select_collection(self.db_service.patients_repository)
            driver.collection.remove()
        self.db_service.index_service.connect()
        self.db_service.index_service.session.execute('drop database %s' %
                                                      self.db_service.index_service.db)
        self.db_service.index_service.disconnect()

    def run(self, patients_size, ehr_size, run_on_clean_dataset=True, build_dataset_threads=1):
        min_records = self.matching_instances['blood_pressure'] + self.matching_instances['urin_analysis']\
                      - self.matching_instances['intersect']
        if min_records > patients_size:
            raise ValueError("Patient size must be greater or equal than matching instances")
        if run_on_clean_dataset:
            self.logger.info('Running tests on a cleaned up dataset')
            self.cleanup()
        try:
            self.logger.info('Creating dataset. %d patients and %d EHRs for patient' %
                             (patients_size, ehr_size))
            _, build_dataset_time = self.build_dataset(patients_size, ehr_size,
                                                       build_dataset_threads)
            # self.logger.info('Running "SELECT ALL" query')
            # _, select_all_time = self.execute_select_all_query()
            # self.logger.info('Running "SELECT ALL" filtered by patient query')
            # _, select_all_patient_time = self.execute_select_all_patient_query()
            # self.logger.info('Running filtered query')
            # _, filtered_query_time = self.execute_filtered_query()
            # self.logger.info('Running filtered with patient filter')
            # _, filtered_patient_time = self.execute_patient_filtered_query()
            self.logger.info('Running patient_count_query')
            bp_patient_count_results, bp_patient_count_time = self.blood_pressure_execute_patient_count_query()
            if not len(list(bp_patient_count_results.get_distinct_results('patient_identifier'))) == \
                self.matching_instances['blood_pressure']:
                self.logger.warning('BLOOD PRESSURE COUNT: query doesn\'t match expected results')
            ua_patient_count_results, ua_patient_count_time = self.urine_analysis_execute_patient_count_query()
            if not len(list(ua_patient_count_results.get_distinct_results('patient_identifier'))) == \
                self.matching_instances['urin_analysis']:
                self.logger.warning('URIN ANALYSIS COUNT: query doesn\'t match expected results')
            intersection_count, intersection_time = self.blood_pressure_urine_analysis_intersection()
            if not len(intersection_count) == self.matching_instances['intersect']:
                self.logger.warning('INTERSECTION COUNT: query doesn\'t match expected results')
        except Exception, e:
            import traceback
            traceback.print_exc(e)
            self.logger.critical('An error has occurred, cleaning up dataset')
            self.cleanup()
            raise e
        return bp_patient_count_time, ua_patient_count_time, intersection_time


def get_parser():
    parser = argparse.ArgumentParser('Run the test_query_performance tool')
    parser.add_argument('--conf-file', type=str, required=True,
                        help='pyEHR configuration file')
    parser.add_argument('--patients-size', type=int, default=10,
                        help='The number of PatientRecords that will be created for the test')
    parser.add_argument('--ehrs-size', type=int, default=10,
                        help='The number of EHR records that will be created for each patient')
    parser.add_argument('--cleanup-dataset', action='store_true',
                        help='Run tests on a cleaned up dataset')
    parser.add_argument('--log-file', type=str, help='LOG file (default stderr)')
    parser.add_argument('--log-level', type=str, default='INFO',
                        help='LOG level (default INFO)')
    parser.add_argument('--archetype-dir', type=str, required=True,
                        help='The directory containing archetype in json format')
    parser.add_argument('--structures-description-file', type=str, required=True,
                        help='JSON file with the description of the structures that will be used to produce the EHRs')
    parser.add_argument('--build-dataset-threads', type=int, default=1,
                        help='The number of threads that will be used to create the dataset (default 1)')
    parser.add_argument('--bp-matching-instances', type=int, default=100,
                        help='The number of Blood Pressure records that will match the test query')
    parser.add_argument('--au-matching-instances', type=int, default=100,
                        help='The number of Urine Analysys records that will match the test query')
    parser.add_argument('--i-matching-instances', type=int, default=100,
                        help='The number of records that will match both Blood Pressure and Urine Analysis queries')
    return parser


def main(argv):
    parser = get_parser()
    args = parser.parse_args(argv)
    matching_instances = {
        'blood_pressure': args.bp_matching_instances,
        'urin_analysis': args.ua_matching_instances,
        'intersect': args.i_matching_instances
    }
    qpt = QueryPerformanceTest(args.conf_file, args.archetype_dir, args.structures_description_file,
                               matching_instances, args.log_file, args.log_level)
    qpt.logger.info('--- STARTING TESTS ---')
    qpt.run(args.patients_size, args.ehrs_size, args.cleanup_dataset, args.build_dataset_threads)
    qpt.logger.info('--- DONE WITH TESTS ---')


if __name__ == '__main__':
    main(sys.argv[1:])