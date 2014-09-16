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
        self._delete_index_db()
        self.patients = None

    def _delete_index_db(self):
        self.dbs.index_service.connect()
        self.dbs.index_service.session.execute('drop database %s' %
                                               self.dbs.index_service.db)
        self.dbs.index_service.disconnect()

    def _get_blood_pressure_data(self, systolic=None, dyastolic=None, mean_arterial=None, pulse=None):
        archetype_id = 'openEHR-EHR-OBSERVATION.blood_pressure.v1'
        bp_doc = {"data": {"at0001": [{"events": [{"at0006": {"data": {"at0003": [{"items": {}}]}}}]}]}}
        if systolic:
            bp_doc['data']['at0001'][0]['events'][0]['at0006']['data']['at0003'][0]['items']['at0004'] = \
                {'value': systolic}
        if dyastolic:
            bp_doc['data']['at0001'][0]['events'][0]['at0006']['data']['at0003'][0]['items']['at0005'] = \
                {'value': dyastolic}
        if mean_arterial:
            bp_doc['data']['at0001'][0]['events'][0]['at0006']['data']['at0003'][0]['items']['at1006'] = \
                {'value': mean_arterial}
        if pulse:
            bp_doc['data']['at0001'][0]['events'][0]['at0006']['data']['at0003'][0]['items']['at1007'] = \
                {'value': pulse}
        return archetype_id, bp_doc

    def _build_patients_batch(self, num_patients, num_ehr, systolic_range=None, dyastolic_range=None):
        records_details = dict()
        for x in xrange(0, num_patients):
            p = self.dbs.save_patient(PatientRecord('PATIENT_%02d' % x))
            crecs = list()
            for y in xrange(0, num_ehr):
                systolic = randint(*systolic_range) if systolic_range else None
                dyastolic = randint(*dyastolic_range) if dyastolic_range else None
                bp_arch = ArchetypeInstance(*self._get_blood_pressure_data(systolic, dyastolic))
                crecs.append(ClinicalRecord(bp_arch))
                records_details.setdefault(p.record_id, []).append({'systolic': systolic, 'dyastolic': dyastolic})
            for c in crecs:
                _, p = self.dbs.save_ehr_record(c, p)
            # _, p, _ = self.dbs.save_ehr_records(crecs, p)
            self.patients.append(p)
        return records_details

    def test_simple_select_query(self):
        query = """
        SELECT o/data[at0001]/events[at0006]/data[at0003]/items[at0004]/value AS systolic,
        o/data[at0001]/events[at0006]/data[at0003]/items[at0005]/value AS dyastolic
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

    def test_simple_where_query(self):
        query = """
        SELECT o/data[at0001]/events[at0006]/data[at0003]/items[at0004]/value AS systolic,
        o/data[at0001]/events[at0006]/data[at0003]/items[at0005]/value AS dyastolic
        FROM Ehr e
        CONTAINS Observation o[openEHR-EHR-OBSERVATION.blood_pressure.v1]
        WHERE o/data[at0001]/events[at0006]/data[at0003]/items[at0004]/value >= 180
        OR o/data[at0001]/events[at0006]/data[at0003]/items[at0005]/value >= 110
        """
        batch_details = self._build_patients_batch(10, 10, (0, 250), (0, 200))
        results = self.qmanager.execute_aql_query(query)
        details_results = list()
        for k, v in batch_details.iteritems():
            for x in v:
                if x['systolic'] >= 180 or x['dyastolic'] >= 110:
                    details_results.append(x)
        res = list(results.results)
        self.assertEqual(sorted(details_results), sorted(res))

    def test_simple_parametric_query(self):
        query = """
        SELECT o/data[at0001]/events[at0006]/data[at0003]/items[at0004]/value AS systolic,
        o/data[at0001]/events[at0006]/data[at0003]/items[at0005]/value AS dyastolic
        FROM Ehr e [uid=$ehrUid]
        CONTAINS Observation o[openEHR-EHR-OBSERVATION.blood_pressure.v1]
        """
        batch_details = self._build_patients_batch(10, 10, (50, 100), (50, 100))
        for patient_label, records in batch_details.iteritems():
            results = self.qmanager.execute_aql_query(query, {'ehrUid': patient_label})
            res = list(results.results)
            self.assertEqual(sorted(records), sorted(res))


def suite():
    suite = unittest.TestSuite()
    suite.addTest(TestQueryManager('test_simple_select_query'))
    suite.addTest(TestQueryManager('test_simple_where_query'))
    suite.addTest(TestQueryManager('test_simple_parametric_query'))
    return suite

if __name__ == '__main__':
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite())