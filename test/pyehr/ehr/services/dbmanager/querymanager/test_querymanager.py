import unittest, os, sys
from random import randint
from pyehr.ehr.services.dbmanager.querymanager import QueryManager
from pyehr.ehr.services.dbmanager.dbservices import DBServices
from pyehr.ehr.services.dbmanager.dbservices.wrappers import PatientRecord,\
    ClinicalRecord, ArchetypeInstance
from pyehr.utils.services import get_service_configuration

CONF_FILE = os.getenv('SERVICE_CONFIG_FILE')


class TestQueryManager(unittest.TestCase):

    def __init__(self, label):
        super(TestQueryManager, self).__init__(label)

    def setUp(self):
        if CONF_FILE is None:
            sys.exit('ERROR: no configuration file provided')
        sconf = get_service_configuration(CONF_FILE)
        self.dbs = DBServices(**sconf.get_db_configuration())
        self.dbs.set_index_service(**sconf.get_index_configuration())
        self.qmanager = QueryManager(**sconf.get_db_configuration())
        self.qmanager.set_index_service(**sconf.get_index_configuration())
        self.patients = list()

    def tearDown(self):
        for p in self.patients:
            self.dbs.delete_patient(p, cascade_delete=True)
        self.patients = None
        pass

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

    def test_simple_select_query(self):
        query = """
        SELECT o/data[at0001]/events[at0006]/data[at0003]/items[at0004]/value/magnitude AS systolic,
        o/data[at0001]/events[at0006]/data[at0003]/items[at0005]/value/magnitude AS diastolic
        FROM Ehr e
        CONTAINS Observation o[openEHR-EHR-OBSERVATION.blood_pressure.v1]
        """
        batch_details = self._build_patients_batch(10, 10, (50, 100), (50, 100))
        results = self.qmanager.execute_aql_query(query)
        details_results = list()
        for k, v in batch_details.iteritems():
            details_results.extend(v)
        res = list(results.results)
        self.assertEqual(sorted(details_results), sorted(res))

    def test_deep_select_query(self):
        query = """
        SELECT o/data[at0001]/events[at0006]/data[at0003]/items[at0004]/value/magnitude AS systolic,
        o/data[at0001]/events[at0006]/data[at0003]/items[at0005]/value/magnitude AS diastolic
        FROM Ehr e
        CONTAINS Composition c[openEHR-EHR-COMPOSITION.encounter.v1]
        CONTAINS Observation o[openEHR-EHR-OBSERVATION.blood_pressure.v1]
        """
        batch_details = self._build_patients_batch_mixed(10, 10, (50, 100), (50, 100))
        results = self.qmanager.execute_aql_query(query)
        details_results = list()
        for k, v in batch_details.iteritems():
            details_results.extend(v)
        res = list(results.results)
        self.assertEqual(sorted(details_results), sorted(res))

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
        results = self.qmanager.execute_aql_query(query)
        details_results = list()
        for k, v in batch_details.iteritems():
            for x in v:
                if x['systolic'] >= 180 or x['diastolic'] >= 110:
                    details_results.append(x)
        res = list(results.results)
        self.assertEqual(sorted(details_results), sorted(res))

    def test_single_where_query2(self):
        query = """
        SELECT o/data[at0001]/events[at0006]/data[at0003]/items[at0004]/value/magnitude AS systolic,
        o/data[at0001]/events[at0006]/data[at0003]/items[at0005]/value/magnitude AS diastolic
        FROM Ehr e
        CONTAINS Observation o[openEHR-EHR-OBSERVATION.blood_pressure.v1]
        WHERE o/data[at0001]/events[at0006]/data[at0003]/items[at0004]/value/magnitude != 180
        """
        batch_details = self._build_patients_batch(10, 10, (0, 250), (0, 200))
        results = self.qmanager.execute_aql_query(query)
        details_results = list()
        for k, v in batch_details.iteritems():
            for x in v:
                if x['systolic'] != 180:
                    details_results.append(x)
        res = list(results.results)
        self.assertEqual(sorted(details_results), sorted(res))

    def test_single_where_query(self):
        query = """
        SELECT o/data[at0001]/events[at0006]/data[at0003]/items[at0004]/value/magnitude AS systolic,
        o/data[at0001]/events[at0006]/data[at0003]/items[at0005]/value/magnitude AS diastolic
        FROM Ehr e
        CONTAINS Observation o[openEHR-EHR-OBSERVATION.blood_pressure.v1]
        WHERE o/data[at0001]/events[at0006]/data[at0003]/items[at0004]/value/magnitude >= 180
        """
        batch_details = self._build_patients_batch(10, 10, (0, 250), (0, 200))
        results = self.qmanager.execute_aql_query(query)
        details_results = list()
        for k, v in batch_details.iteritems():
            for x in v:
                if x['systolic'] >= 180:
                    details_results.append(x)
        res = list(results.results)
        self.assertEqual(sorted(details_results), sorted(res))

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
        results = self.qmanager.execute_aql_query(query)
        details_results = list()
        for k, v in batch_details.iteritems():
            for x in v:
                if x['systolic'] >= 180 or x['diastolic'] >= 110:
                    details_results.append(x)
        res = list(results.results)
        self.assertEqual(sorted(details_results), sorted(res))

    def test_deep_where_query2(self):
        query = """
        SELECT o/data[at0001]/events[at0006]/data[at0003]/items[at0004]/value/magnitude AS systolic,
        o/data[at0001]/events[at0006]/data[at0003]/items[at0005]/value/magnitude AS diastolic
        FROM Ehr e
        CONTAINS Composition c[openEHR-EHR-COMPOSITION.encounter.v1]
        CONTAINS Observation o[openEHR-EHR-OBSERVATION.blood_pressure.v1]
        WHERE o/data[at0001]/events[at0006]/data[at0003]/items[at0004]/value/magnitude >= 180
        OR o/data[at0001]/events[at0006]/data[at0003]/items[at0005]/value/magnitude != 110
        """
        batch_details = self._build_patients_batch_mixed(10, 10, (0, 250), (109, 110))
        results = self.qmanager.execute_aql_query(query)
        details_results = list()
        for k, v in batch_details.iteritems():
            for x in v:
                if x['systolic'] >= 180 or x['diastolic'] != 110:
                    details_results.append(x)
        res = list(results.results)
        self.assertEqual(sorted(details_results), sorted(res))

    def test_deeper_where_query(self):
        query = """
        SELECT o/data[at0001]/events[at0006]/data[at0003]/items[at0004]/value/magnitude AS systolic,
        o/data[at0001]/events[at0006]/data[at0003]/items[at0005]/value/magnitude AS diastolic
        FROM Ehr e
        CONTAINS Composition c[openEHR-EHR-COMPOSITION.encounter.v1]
        CONTAINS Observation o[openEHR-EHR-OBSERVATION.blood_pressure.v1]
        WHERE o/data[at0001]/events[at0006]/data[at0003]/items[at0004]/value/magnitude >= 180
        AND o/data[at0001]/events[at0006]/data[at0003]/items[at0005]/value/magnitude != 81
        """
        batch_details = self._build_patients_batch_mixed(10, 10, (0, 250), (0, 200))
        pass
        results = self.qmanager.execute_aql_query(query)
        details_results = list()
        for k, v in batch_details.iteritems():
            for x in v:
                if x['systolic'] >= 180 and x['diastolic'] != 81:
                    details_results.append(x)
        res = list(results.results)
        self.assertEqual(sorted(details_results), sorted(res))

    def test_simple_parametric_query(self):
        query = """
        SELECT o/data[at0001]/events[at0006]/data[at0003]/items[at0004]/value/magnitude AS systolic,
        o/data[at0001]/events[at0006]/data[at0003]/items[at0005]/value/magnitude AS diastolic
        FROM Ehr e [uid=$ehrUid]
        CONTAINS Observation o[openEHR-EHR-OBSERVATION.blood_pressure.v1]
        """
        batch_details = self._build_patients_batch(10, 10, (50, 100), (50, 100))
        for patient_label, records in batch_details.iteritems():
            results = self.qmanager.execute_aql_query(query, {'ehrUid': patient_label})
            res = list(results.results)
            self.assertEqual(sorted(records), sorted(res))

    def test_simple_patients_selection(self):
        query = """
        SELECT e/ehr_id/value AS patient_identifier
        FROM Ehr e
        CONTAINS Observation o[openEHR-EHR-OBSERVATION.blood_pressure.v1]
        WHERE o/data[at0001]/events[at0006]/data[at0003]/items[at0004]/value/magnitude >= 180
        OR o/data[at0001]/events[at0006]/data[at0003]/items[at0005]/value/magnitude >= 110
        """
        batch_details = self._build_patients_batch(10, 10, (0, 250), (0, 200))
        results = self.qmanager.execute_aql_query(query)
        details_results = set()
        for k, v in batch_details.iteritems():
            for x in v:
                if x['systolic'] >= 180 or x['diastolic'] >= 110:
                    details_results.add(k)
        res = list(results.get_distinct_results('patient_identifier'))
        self.assertEqual(sorted(list(details_results)), sorted(res))

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
        results = self.qmanager.execute_aql_query(query, count_only=True)
        results_count = 0
        for k, v in batch_details.iteritems():
            for x in v:
                if x['systolic'] >= 180 or x['diastolic'] >= 110:
                    results_count += 1
        self.assertEqual(results_count, results)

    def test_multiprocess_query(self):
        query = """
        SELECT o/data[at0001]/events[at0006]/data[at0003]/items[at0004]/value/magnitude AS systolic,
        o/data[at0001]/events[at0006]/data[at0003]/items[at0005]/value/magnitude AS diastolic
        FROM Ehr e
        CONTAINS Observation o[openEHR-EHR-OBSERVATION.blood_pressure.v1]
        WHERE o/data[at0001]/events[at0006]/data[at0003]/items[at0004]/value/magnitude >= 180
        OR o/data[at0001]/events[at0006]/data[at0003]/items[at0005]/value/magnitude >= 110
        """
        _ = self._build_patients_batch_mixed(10, 10, (0, 250), (0, 200))
        sp_results = self.qmanager.execute_aql_query(query)
        mp_results = self.qmanager.execute_aql_query(query, query_processes=2)
        self.assertEqual(sorted(sp_results.to_json()), sorted(mp_results.to_json()))


def suite():
    suite = unittest.TestSuite()
    suite.addTest(TestQueryManager('test_simple_select_query'))
    suite.addTest(TestQueryManager('test_simple_where_query'))
    suite.addTest(TestQueryManager('test_single_where_query'))
    suite.addTest(TestQueryManager('test_single_where_query2'))
    suite.addTest(TestQueryManager('test_deep_where_query'))
    suite.addTest(TestQueryManager('test_deep_where_query2'))
    suite.addTest(TestQueryManager('test_deeper_where_query'))
    suite.addTest(TestQueryManager('test_simple_parametric_query'))
    suite.addTest(TestQueryManager('test_simple_patients_selection'))
    suite.addTest(TestQueryManager('test_deep_select_query'))
    suite.addTest(TestQueryManager('test_count_query'))
    suite.addTest(TestQueryManager('test_multiprocess_query'))
    return suite

if __name__ == '__main__':
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite())