import os, unittest, requests, sys, json
from random import randint
from pyehr.utils.services import get_service_configuration
from pyehr.ehr.services.dbmanager.dbservices import DBServices
from pyehr.ehr.services.dbmanager.dbservices.wrappers import ArchetypeInstance,\
    ClinicalRecord, PatientRecord
from pyehr.utils import decode_dict

CONF_FILE = os.getenv('SERVICE_CONFIG_FILE')


class TestQueryService(unittest.TestCase):

    def __init__(self, label):
        super(TestQueryService, self).__init__(label)
        if CONF_FILE is None:
            sys.exit('ERROR: no configuration file provided')
        sconf = get_service_configuration(CONF_FILE)
        self.qservice_uri = 'http://%s:%s' % (sconf.get_query_service_configuration()['host'],
                                              sconf.get_query_service_configuration()['port'])
        self.query_path = 'query/execute'
        self.query_count_path = 'query/execute_count'

    def setUp(self):
        sconf = get_service_configuration(CONF_FILE)
        self.dbs = DBServices(**sconf.get_db_configuration())
        self.dbs.set_index_service(**sconf.get_index_configuration())
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

    def _get_blood_pressure_data(self, systolic=None, diastolic=None, mean_arterial=None, pulse=None):
        archetype_id = 'openEHR-EHR-OBSERVATION.blood_pressure.v1'
        bp_doc = {"data": {"at0001": [{"events": [{"at0006": {"data": {"at0003": [{"items": {}}]}}}]}]}}
        if systolic is not None:
            bp_doc['data']['at0001'][0]['events'][0]['at0006']['data']['at0003'][0]['items']['at0004'] = \
                {'value': self._get_quantity(systolic, 'mm[Hg]')}
        if diastolic is not None:
            bp_doc['data']['at0001'][0]['events'][0]['at0006']['data']['at0003'][0]['items']['at0005'] = \
                {'value': self._get_quantity(diastolic, 'mm[Hg]')}
        if mean_arterial is not None:
            bp_doc['data']['at0001'][0]['events'][0]['at0006']['data']['at0003'][0]['items']['at1006'] = \
                {'value': self._get_quantity(mean_arterial, 'mm[Hg]')}
        if pulse is not None:
            bp_doc['data']['at0001'][0]['events'][0]['at0006']['data']['at0003'][0]['items']['at1007'] = \
                {'value': self._get_quantity(pulse, 'mm[Hg]')}
        return archetype_id, bp_doc

    def _get_encounter_data(self, archetypes):
        archetype_id = 'openEHR-EHR-COMPOSITION.encounter.v1'
        enc_doc = {'context': {'event_context': {'other_context': {'at0001': [{'items': {'at0002': archetypes}}]}}}}
        return archetype_id, enc_doc

    def _build_patients_batch(self, num_patients, num_ehr, systolic_range=None, diastolic_range=None):
        records_details = dict()
        for x in xrange(0, num_patients):
            p = self.dbs.save_patient(PatientRecord('PATIENT_%02d' % x))
            crecs = list()
            for y in xrange(0, num_ehr):
                systolic = randint(*systolic_range) if systolic_range else None
                diastolic = randint(*diastolic_range) if diastolic_range else None
                bp_arch = ArchetypeInstance(*self._get_blood_pressure_data(systolic, diastolic))
                crecs.append(ClinicalRecord(bp_arch))
                records_details.setdefault(p.record_id, []).append({'systolic': systolic, 'diastolic': diastolic})
            _, p, _ = self.dbs.save_ehr_records(crecs, p)
            self.patients.append(p)
        return records_details

    def _build_patients_batch_mixed(self, num_patients, num_ehr, systolic_range=None, diastolic_range=None):
        records_details = dict()
        for x in xrange(0, num_patients):
            p = self.dbs.save_patient(PatientRecord('PATIENT_%02d' % x))
            crecs = list()
            for y in xrange(0, num_ehr):
                systolic = randint(*systolic_range) if systolic_range else None
                diastolic = randint(*diastolic_range) if diastolic_range else None
                bp_arch = ArchetypeInstance(*self._get_blood_pressure_data(systolic, diastolic))
                if randint(0, 2) == 1:
                    crecs.append(ClinicalRecord(ArchetypeInstance(*self._get_encounter_data([bp_arch]))))
                    records_details.setdefault(p.record_id, []).append({'systolic': systolic, 'diastolic': diastolic})
                else:
                    crecs.append(ClinicalRecord(bp_arch))
            _, p, _ = self.dbs.save_ehr_records(crecs, p)
            self.patients.append(p)
        return records_details

    def _get_path(self, query_path):
        return '/'.join([self.qservice_uri, query_path])

    def _build_query_request(self, query, query_params=None):
        return {
            'query': query,
            'query_params': json.dumps(query_params)
        }

    def test_count_query(self):
        query = """
        SELECT o/data[at0001]/events[at0006]/data[at0003]/items[at0004]/value/magnitude AS systolic,
        o/data[at0001]/events[at0006]/data[at0003]/items[at0005]/value/magnitude AS diastolic
        FROM Ehr e
        CONTAINS Observation o[openEHR-EHR-OBSERVATION.blood_pressure.v1]
        WHERE o/data[at0001]/events[at0006]/data[at0003]/items[at0004]/value/magnitude >= 180
        OR o/data[at0001]/events[at0006]/data[at0003]/items[at0005]/value/magnitude >= 110
        """
        batch_details = self._build_patients_batch(10, 10, (0, 250), (0, 200))
        query_request = self._build_query_request(query)
        results = requests.post(self._get_path(self.query_count_path), query_request)
        self.assertEqual(results.status_code, requests.codes.ok)
        self.assertTrue(results.json()['SUCCESS'])
        results_count = 0
        for k, v in batch_details.iteritems():
            for x in v:
                if x['systolic'] >= 180 or x['diastolic'] >= 110:
                    results_count += 1
        self.assertEqual(results_count, results.json()['RESULTS_COUNTER'])

    def test_simple_select_query(self):
        query = """
        SELECT o/data[at0001]/events[at0006]/data[at0003]/items[at0004]/value/magnitude AS systolic,
        o/data[at0001]/events[at0006]/data[at0003]/items[at0005]/value/magnitude AS diastolic
        FROM Ehr e
        CONTAINS Observation o[openEHR-EHR-OBSERVATION.blood_pressure.v1]
        """
        batch_details = self._build_patients_batch(10, 10, (50, 100), (50, 100))
        query_request = self._build_query_request(query)
        results = requests.post(self._get_path(self.query_path), query_request)
        self.assertEqual(results.status_code, requests.codes.ok)
        self.assertTrue(results.json()['SUCCESS'])
        results_set = decode_dict(results.json()['RESULTS_SET'])
        details_results = list()
        for k, v in batch_details.iteritems():
            details_results.extend(v)
        self.assertEqual(sorted(results_set['results']), sorted(details_results))

    def test_deep_select_query(self):
        query = """
        SELECT o/data[at0001]/events[at0006]/data[at0003]/items[at0004]/value/magnitude AS systolic,
        o/data[at0001]/events[at0006]/data[at0003]/items[at0005]/value/magnitude AS diastolic
        FROM Ehr e
        CONTAINS Composition c[openEHR-EHR-COMPOSITION.encounter.v1]
        CONTAINS Observation o[openEHR-EHR-OBSERVATION.blood_pressure.v1]
        """
        batch_details = self._build_patients_batch_mixed(10, 10, (50, 100), (50, 100))
        query_request = self._build_query_request(query)
        results = requests.post(self._get_path(self.query_path), query_request)
        self.assertEqual(results.status_code, requests.codes.ok)
        self.assertTrue(results.json()['SUCCESS'])
        results_set = decode_dict(results.json()['RESULTS_SET'])
        details_results = list()
        for k, v in batch_details.iteritems():
            details_results.extend(v)
        self.assertEqual(sorted(results_set['results']), sorted(details_results))

    def test_simple_where_query(self):
        query = """
        SELECT o/data[at0001]/events[at0006]/data[at0003]/items[at0004]/value/magnitude AS systolic,
        o/data[at0001]/events[at0006]/data[at0003]/items[at0005]/value/magnitude AS diastolic
        FROM Ehr e
        CONTAINS Observation o[openEHR-EHR-OBSERVATION.blood_pressure.v1]
        WHERE o/data[at0001]/events[at0006]/data[at0003]/items[at0004]/value/magnitude >= 180
        OR o/data[at0001]/events[at0006]/data[at0003]/items[at0005]/value/magnitude >= 110
        """
        batch_details = self._build_patients_batch(10, 10, (0, 250), (0, 200))
        query_request = self._build_query_request(query)
        results = requests.post(self._get_path(self.query_path), query_request)
        self.assertEqual(results.status_code, requests.codes.ok)
        self.assertTrue(results.json()['SUCCESS'])
        results_set = decode_dict(results.json()['RESULTS_SET'])
        details_results = list()
        for k, v in batch_details.iteritems():
            for x in v:
                if x['systolic'] >= 180 or x['diastolic'] >= 110:
                    details_results.append(x)
        self.assertEqual(sorted(results_set['results']), sorted(details_results))

    def test_deep_where_query(self):
        query = """
        SELECT o/data[at0001]/events[at0006]/data[at0003]/items[at0004]/value/magnitude AS systolic,
        o/data[at0001]/events[at0006]/data[at0003]/items[at0005]/value/magnitude AS diastolic
        FROM Ehr e
        CONTAINS Composition c[openEHR-EHR-COMPOSITION.encounter.v1]
        CONTAINS Observation o[openEHR-EHR-OBSERVATION.blood_pressure.v1]
        WHERE o/data[at0001]/events[at0006]/data[at0003]/items[at0004]/value/magnitude >= 180
        OR o/data[at0001]/events[at0006]/data[at0003]/items[at0005]/value/magnitude >= 110
        """
        batch_details = self._build_patients_batch_mixed(10, 10, (0, 250), (0, 200))
        query_request = self._build_query_request(query)
        results = requests.post(self._get_path(self.query_path), query_request)
        self.assertEqual(results.status_code, requests.codes.ok)
        self.assertTrue(results.json()['SUCCESS'])
        results_set = decode_dict(results.json()['RESULTS_SET'])
        details_results = list()
        for k, v in batch_details.iteritems():
            for x in v:
                if x['systolic'] >= 180 or x['diastolic'] >= 110:
                    details_results.append(x)
        self.assertEqual(sorted(results_set['results']), sorted(details_results))

    def test_simple_parametric_query(self):
        query = """
        SELECT o/data[at0001]/events[at0006]/data[at0003]/items[at0004]/value/magnitude AS systolic,
        o/data[at0001]/events[at0006]/data[at0003]/items[at0005]/value/magnitude AS diastolic
        FROM Ehr e [uid=$ehrUid]
        CONTAINS Observation o[openEHR-EHR-OBSERVATION.blood_pressure.v1]
        """
        batch_details = self._build_patients_batch(10, 10, (50, 100), (50, 100))
        for patient_label, records in batch_details.iteritems():
            query_request = self._build_query_request(query, {'ehrUid': patient_label})
            results = requests.post(self._get_path(self.query_path), query_request)
            self.assertEqual(results.status_code, requests.codes.ok)
            self.assertTrue(results.json()['SUCCESS'])
            results_set = decode_dict(results.json()['RESULTS_SET'])
            self.assertEqual(sorted(records), sorted(results_set['results']))

    def test_simple_patients_selection(self):
        query = """
        SELECT e/ehr_id/value AS patient_identifier
        FROM Ehr e
        CONTAINS Observation o[openEHR-EHR-OBSERVATION.blood_pressure.v1]
        WHERE o/data[at0001]/events[at0006]/data[at0003]/items[at0004]/value/magnitude >= 180
        OR o/data[at0001]/events[at0006]/data[at0003]/items[at0005]/value/magnitude >= 110
        """
        batch_details = self._build_patients_batch(10, 10, (0, 250), (0, 200))
        query_request = self._build_query_request(query)
        results = requests.post(self._get_path(self.query_path), query_request)
        self.assertEqual(results.status_code, requests.codes.ok)
        self.assertTrue(results.json()['SUCCESS'])
        results_set = decode_dict(results.json()['RESULTS_SET'])
        details_results = list()
        for k, v in batch_details.iteritems():
            for x in v:
                if x['systolic'] >= 180 or x['diastolic'] >= 110:
                    details_results.append({'patient_identifier': k})
        self.assertEqual(sorted(results_set['results']), sorted(details_results))


def suite():
    suite = unittest.TestSuite()
    suite.addTest(TestQueryService('test_count_query'))
    suite.addTest(TestQueryService('test_simple_select_query'))
    suite.addTest(TestQueryService('test_deep_select_query'))
    suite.addTest(TestQueryService('test_simple_where_query'))
    suite.addTest(TestQueryService('test_deep_where_query'))
    suite.addTest(TestQueryService('test_simple_parametric_query'))
    suite.addTest(TestQueryService('test_simple_patients_selection'))
    return suite

if __name__ == '__main__':
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite())