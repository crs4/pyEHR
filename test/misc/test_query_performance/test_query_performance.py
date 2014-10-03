import time, sys, argparse, json
from random import randint
from functools import wraps

from pyehr.ehr.services.dbmanager.dbservices.wrappers import ClinicalRecord, PatientRecord
from pyehr.ehr.services.dbmanager.dbservices import DBServices
from pyehr.ehr.services.dbmanager.querymanager import QueryManager
from pyehr.utils.services import get_service_configuration, get_logger

from structures_builder import build_record


class QueryPerformanceTest(object):

    def __init__(self, pyehr_conf_file, archetypes_dir, structures_description_file,
                 log_file=None, log_level='INFO', db_name_prefix=None):
        self.logger = get_logger('query_performance_test',
                         log_file=log_file, log_level=log_level)
        sconf = get_service_configuration(pyehr_conf_file)
        db_conf = sconf.get_db_configuration()
        index_conf = sconf.get_index_configuration()
        if db_name_prefix:
            db_conf['database'] = '%s_%s' % (db_conf['database'], db_name_prefix)
            index_conf['database'] = '%s_%s' % (index_conf['database'], db_name_prefix)
        self.db_service = DBServices(**db_conf)
        self.db_service.set_index_service(**index_conf)
        self.query_manager = QueryManager(**db_conf)
        self.query_manager.set_index_service(**index_conf)
        self.archetypes_dir = archetypes_dir
        self.structures_file = structures_description_file

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
    def build_dataset(self, patients, ehrs):
        def records_by_chunk(records, batch_size=500):
            offset = 0
            while len(records[offset:]) > 0:
                yield records[offset:offset+batch_size]
                offset += batch_size

        with open(self.structures_file) as f:
            structures = json.loads(f.read())
        for x in xrange(0, patients):
            crecs = list()
            p = self.db_service.get_patient('PATIENT_%05d' % x, fetch_ehr_records=False)
            if p is None:
                p = self.db_service.save_patient(PatientRecord('PATIENT_%05d' % x))
                self.logger.debug('Saved patient PATIENT_%05d', x)
            else:
                self.logger.debug('Patient PATIENT_%05d already exists, using it', x)
            for i in xrange(ehrs - len(p.ehr_records)):
                st = structures[randint(0, len(structures)-1)]
                arch = build_record(st, self.archetypes_dir)
                crecs.append(ClinicalRecord(arch))
            self.logger.debug('Done building %d EHRs', (ehrs - len(p.ehr_records)))
            for chunk in records_by_chunk(crecs):
                self.db_service.save_ehr_records(chunk, p)
            self.logger.debug('EHRs saved')
        drf = self.db_service._get_drivers_factory(self.db_service.ehr_repository)
        with drf.get_driver() as driver:
            self.logger.info('*** Produced %d different structures ***',
                             len(driver.collection.distinct('ehr_structure_id')))

    def execute_query(self, query, params=None):
        results = self.query_manager.execute_aql_query(query, params)
        self.logger.info('Retrieved %d records' % results.total_results)

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
        SELECT o/data[at0001]/events[at0006]/data[at0003]/items[at0004]/value/magnitude,
        o/data[at0001]/events[at0006]/data[at0003]/items[at0005]/value/magnitude
        FROM Ehr e
        CONTAINS Observation o[openEHR-EHR-OBSERVATION.blood_pressure.v1]
        WHERE o/data[at0001]/events[at0006]/data[at0003]/items[at0004]/value/magnitude >= 180
        OR o/data[at0001]/events[at0006]/data[at0003]/items[at0005]/value/magnitude >= 110
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
    def execute_patient_count_query(self):
        query = """
        SELECT e/ehr_id/value AS patient_identifier
        FROM Ehr e
        CONTAINS Observation o[openEHR-EHR-OBSERVATION.blood_pressure.v1]
        WHERE o/data[at0001]/events[at0006]/data[at0003]/items[at0004]/value/magnitude >= 180
        OR o/data[at0001]/events[at0006]/data[at0003]/items[at0005]/value/magnitude >= 110
        """
        return self.execute_query(query)

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

    def run(self, patients_size, ehr_size, run_on_clean_dataset=True):
        if run_on_clean_dataset:
            self.logger.info('Running tests on a cleaned up dataset')
            self.cleanup()
        try:
            self.logger.info('Creating dataset. %d patients and %d EHRs for patient' % (patients_size,
                                                                            ehr_size))
            _, build_dataset_time = self.build_dataset(patients_size, ehr_size)
            self.logger.info('Running "SELECT ALL" query')
            _, select_all_time = self.execute_select_all_query()
            self.logger.info('Running "SELECT ALL" filtered by patient query')
            _, select_all_patient_time = self.execute_select_all_patient_query()
            self.logger.info('Running filtered query')
            _, filtered_query_time = self.execute_filtered_query()
            self.logger.info('Running filtered with patient filter')
            _, filtered_patient_time = self.execute_patient_filtered_query()
            self.logger.info('Running patient_count_query')
            _, patient_count_time = self.execute_patient_count_query()
        except Exception, e:
            self.logger.critical('An error has occurred, cleaning up dataset')
            self.cleanup()
            raise e
        return select_all_time, select_all_patient_time, filtered_query_time,\
               filtered_patient_time, patient_count_time


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
    return parser


def main(argv):
    parser = get_parser()
    args = parser.parse_args(argv)
    qpt = QueryPerformanceTest(args.conf_file, args.archetype_dir, args.structures_description_file,
                               args.log_file, args.log_level)
    qpt.logger.info('--- STARTING TESTS ---')
    qpt.run(args.patients_size, args.ehrs_size, args.cleanup_dataset)
    qpt.logger.info('--- DONE WITH TESTS ---')


if __name__ == '__main__':
    main(sys.argv[1:])