import unittest, os, sys
from random import randint
from pyehr.ehr.services.dbmanager.querymanager.queries_runner import QueriesRunner
from pyehr.ehr.services.dbmanager.dbservices import DBServices
from pyehr.ehr.services.dbmanager.dbservices.wrappers import PatientRecord,\
    ClinicalRecord, ArchetypeInstance
from pyehr.utils.services import get_service_configuration

CONF_FILE = os.getenv('SERVICE_CONFIG_FILE')


class TestQueriesRunner(unittest.TestCase):

    def __init__(self, label):
        super(TestQueriesRunner, self).__init__(label)

    def setUp(self):
        if CONF_FILE is None:
            sys.exit('ERROR: no configuration file provided')
        sconf = get_service_configuration(CONF_FILE)
        db_conf = sconf.get_db_configuration()
        self.dbs = DBServices(**db_conf)
        index_conf = sconf.get_index_configuration()
        self.dbs.set_index_service(**index_conf)
        self.queries_runner = QueriesRunner(db_conf, index_conf)
        self.patients = list()

    def tearDown(self):
        for p in self.patients:
            self.dbs.delete_patient(p, cascade_delete=True)
        self.patients = None

    def _get_quantity(self, value, units):
        return {
            'magnitude': value,
            'units': units
        }

    def _get_blood_pressure_data(self, systolic=None, dyastolic=None, mean_arterial=None, pulse=None):
        archetype_id = 'openEHR-EHR-OBSERVATION.blood_pressure.v1'
        bp_doc = {"data": {"at0001": [{"events": [{"at0006": {"data": {"at0003": [{"items": {}}]}}}]}]}}
        if not systolic is None:
            bp_doc['data']['at0001'][0]['events'][0]['at0006']['data']['at0003'][0]['items']['at0004'] = \
                {'value': self._get_quantity(systolic, 'mm[Hg]')}
        if not dyastolic is None:
            bp_doc['data']['at0001'][0]['events'][0]['at0006']['data']['at0003'][0]['items']['at0005'] = \
                {'value': self._get_quantity(dyastolic, 'mm[Hg]')}
        if not mean_arterial is None:
            bp_doc['data']['at0001'][0]['events'][0]['at0006']['data']['at0003'][0]['items']['at1006'] = \
                {'value': self._get_quantity(mean_arterial, 'mm[Hg]')}
        if not pulse is None:
            bp_doc['data']['at0001'][0]['events'][0]['at0006']['data']['at0003'][0]['items']['at1007'] = \
                {'value': self._get_quantity(pulse, 'mm[Hg]')}
        return archetype_id, bp_doc

    def _build_patients_batch(self, num_patients, num_ehr, first_patient_id=0,
                              systolic_range=None, dyastolic_range=None):
        records_details = dict()
        for x in xrange(first_patient_id, num_patients):
            p = self.dbs.save_patient(PatientRecord('PATIENT_%02d' % x))
            crecs = list()
            for y in xrange(0, num_ehr):
                systolic = randint(*systolic_range) if systolic_range else None
                dyastolic = randint(*dyastolic_range) if dyastolic_range else None
                bp_arch = ArchetypeInstance(*self._get_blood_pressure_data(systolic, dyastolic))
                crecs.append(ClinicalRecord(bp_arch))
                details = {}
                if systolic:
                    details['systolic'] = systolic
                if dyastolic:
                    details['dyastolic'] = dyastolic
                records_details.setdefault(p.record_id, []).append(details)
            _, p, _ = self.dbs.save_ehr_records(crecs, p)
            self.patients.append(p)
        return records_details

    def test_multiple_queries(self):
        sys_batch_details = self._build_patients_batch(10, 10, systolic_range=(100, 250))
        dya_batch_details = self._build_patients_batch(10, 10, dyastolic_range=(100, 250),
                                                       first_patient_id=100)
        sys_query = """
        SELECT o/data[at0001]/events[at0006]/data[at0003]/items[at0004]/value/magnitude AS systolic
        FROM Ehr e
        CONTAINS Observation o[openEHR-EHR-OBSERVATION.blood_pressure.v1]
        WHERE o/data[at0001]/events[at0006]/data[at0003]/items[at0004]/value/magnitude >= 180
        """
        dya_query = """
        SELECT o/data[at0001]/events[at0006]/data[at0003]/items[at0005]/value/magnitude AS dyastolic
        FROM Ehr e
        CONTAINS Observation o[openEHR-EHR-OBSERVATION.blood_pressure.v1]
        WHERE o/data[at0001]/events[at0006]/data[at0003]/items[at0005]/value/magnitude >= 120
        """
        self.queries_runner.add_query('systolic_query', sys_query)
        self.queries_runner.add_query('dyastolic_query', dya_query)
        self.queries_runner.execute_queries()
        sys_expected_results = list()
        for k, v in sys_batch_details.iteritems():
            for x in v:
                if x['systolic'] >= 180:
                    sys_expected_results.append(x)
        res = self.queries_runner.get_result_set('systolic_query')
        self.assertEqual(sorted(sys_expected_results), sorted(res.results))
        dya_expected_results = list()
        for k, v in dya_batch_details.iteritems():
            for x in v:
                if x['dyastolic'] >= 120:
                    dya_expected_results.append(x)
        res = self.queries_runner.get_result_set('dyastolic_query')
        self.assertEqual(sorted(dya_expected_results), sorted(res.results))

    def test_intersection(self):
        intersection_details = self._build_patients_batch(50, 10, systolic_range=(1, 250),
                                                          dyastolic_range=(1, 250))
        for p, values in intersection_details.iteritems():
            print '*** %s ***' % p,
            print values
        sys_query = """
        SELECT e/ehr_id/value AS patient_identifier
        FROM Ehr e
        CONTAINS Observation o[openEHR-EHR-OBSERVATION.blood_pressure.v1]
        WHERE o/data[at0001]/events[at0006]/data[at0003]/items[at0004]/value/magnitude >= 180
        """
        dya_query = """
        SELECT e/ehr_id/value AS patient_identifier
        FROM Ehr e
        CONTAINS Observation o[openEHR-EHR-OBSERVATION.blood_pressure.v1]
        WHERE o/data[at0001]/events[at0006]/data[at0003]/items[at0005]/value/magnitude >= 120
        """
        self.queries_runner.add_query('systolic_query', sys_query)
        self.queries_runner.add_query('dyastolic_query', dya_query)
        self.queries_runner.execute_queries()
        intersect_expected_results = set()
        for k, v in intersection_details.iteritems():
            systolic = False
            dyastolic = False
            for x in v:
                if x['systolic'] >= 180:
                    systolic = True
                if x['dyastolic'] >= 120:
                    dyastolic = True
            if systolic and dyastolic:
                intersect_expected_results.add(k)
        res = self.queries_runner.get_intersection('patient_identifier', 'systolic_query',
                                                   'dyastolic_query')
        self.assertEqual(sorted(intersect_expected_results), sorted(res))

    def test_union(self):
        union_details = self._build_patients_batch(50, 10, systolic_range=(1, 250),
                                                   dyastolic_range=(1, 250))
        sys_query = """
        SELECT e/ehr_id/value AS patient_identifier
        FROM Ehr e
        CONTAINS Observation o[openEHR-EHR-OBSERVATION.blood_pressure.v1]
        WHERE o/data[at0001]/events[at0006]/data[at0003]/items[at0004]/value/magnitude >= 180
        """
        dya_query = """
        SELECT e/ehr_id/value AS patient_identifier
        FROM Ehr e
        CONTAINS Observation o[openEHR-EHR-OBSERVATION.blood_pressure.v1]
        WHERE o/data[at0001]/events[at0006]/data[at0003]/items[at0005]/value/magnitude >= 120
        """
        self.queries_runner.add_query('systolic_query', sys_query)
        self.queries_runner.add_query('dyastolic_query', dya_query)
        self.queries_runner.execute_queries()
        union_expected_results = set()
        for k, v in union_details.iteritems():
            for x in v:
                if x['systolic'] >= 180 or x['dyastolic'] >= 120:
                    union_expected_results.add(k)
        res = self.queries_runner.get_union('patient_identifier', 'systolic_query',
                                            'dyastolic_query')
        self.assertEqual(sorted(union_expected_results), sorted(res))

    def test_cleanup(self):
        self._build_patients_batch(50, 10, systolic_range=(100, 250), dyastolic_range=(100, 250))
        sys_query = """
        SELECT e/ehr_id/value AS patient_identifier
        FROM Ehr e
        CONTAINS Observation o[openEHR-EHR-OBSERVATION.blood_pressure.v1]
        WHERE o/data[at0001]/events[at0006]/data[at0003]/items[at0004]/value/magnitude >= 180
        """
        dya_query = """
        SELECT e/ehr_id/value AS patient_identifier
        FROM Ehr e
        CONTAINS Observation o[openEHR-EHR-OBSERVATION.blood_pressure.v1]
        WHERE o/data[at0001]/events[at0006]/data[at0003]/items[at0005]/value/magnitude >= 120
        """
        self.queries_runner.add_query('systolic_query', sys_query)
        self.queries_runner.add_query('dyastolic_query', dya_query)
        self.assertEqual(self.queries_runner.queries_count, 2)
        self.assertEqual(self.queries_runner.results_count, 0)
        self.queries_runner.execute_queries()
        self.assertEqual(self.queries_runner.results_count, 2)
        self.queries_runner.cleanup()
        self.assertEqual(self.queries_runner.queries_count, 0)
        self.assertEqual(self.queries_runner.results_count, 0)

    def test_remove_query(self):
        self._build_patients_batch(50, 10, systolic_range=(100, 250), dyastolic_range=(100, 250))
        sys_query = """
        SELECT e/ehr_id/value AS patient_identifier
        FROM Ehr e
        CONTAINS Observation o[openEHR-EHR-OBSERVATION.blood_pressure.v1]
        WHERE o/data[at0001]/events[at0006]/data[at0003]/items[at0004]/value/magnitude >= 180
        """
        dya_query = """
        SELECT e/ehr_id/value AS patient_identifier
        FROM Ehr e
        CONTAINS Observation o[openEHR-EHR-OBSERVATION.blood_pressure.v1]
        WHERE o/data[at0001]/events[at0006]/data[at0003]/items[at0005]/value/magnitude >= 120
        """
        self.queries_runner.add_query('systolic_query', sys_query)
        self.queries_runner.add_query('dyastolic_query', dya_query)
        self.queries_runner.execute_queries()
        self.queries_runner.remove_query('systolic_query')
        self.assertIsNone(self.queries_runner.get_result_set('systolic_query'))
        with self.assertRaises(KeyError) as context:
            self.queries_runner.remove_query('systolic_query')


def suite():
    suite = unittest.TestSuite()
    suite.addTest(TestQueriesRunner('test_multiple_queries'))
    suite.addTest(TestQueriesRunner('test_intersection'))
    suite.addTest(TestQueriesRunner('test_union'))
    suite.addTest(TestQueriesRunner('test_cleanup'))
    suite.addTest(TestQueriesRunner('test_remove_query'))
    return suite

if __name__ == '__main__':
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite())