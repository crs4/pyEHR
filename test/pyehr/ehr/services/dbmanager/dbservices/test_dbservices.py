import unittest, sys, os, uuid
from collections import Counter
from pyehr.ehr.services.dbmanager.dbservices import DBServices
from pyehr.ehr.services.dbmanager.dbservices.wrappers import PatientRecord,\
    ClinicalRecord, ArchetypeInstance
from pyehr.ehr.services.dbmanager.errors import DuplicatedKeyError
from pyehr.utils.services import get_service_configuration

CONF_FILE = os.getenv('SERVICE_CONFIG_FILE')


class TestDBServices(unittest.TestCase):

    def __init__(self, label):
        super(TestDBServices, self).__init__(label)

    def create_random_patient(self):
        return PatientRecord(record_id=uuid.uuid4().hex)

    def setUp(self):
        if CONF_FILE is None:
            sys.exit('ERROR: no configuration file provided')
        sconf = get_service_configuration(CONF_FILE)
        self.conf = sconf.get_db_configuration()
        self.index_conf = sconf.get_index_configuration()

    def test_save_patient(self):
        dbs = DBServices(**self.conf)
        dbs.set_index_service(**self.index_conf)
        pat_rec = PatientRecord(record_id='TEST_PATIENT')
        pat_rec = dbs.save_patient(pat_rec)
        self.assertEqual(len(pat_rec.ehr_records), 0)
        self.assertEqual(pat_rec.record_id, 'TEST_PATIENT')
        self.assertTrue(pat_rec.active)
        # cleanup
        dbs.delete_patient(pat_rec)

    def test_save_ehr_record(self):
        dbs = DBServices(**self.conf)
        dbs.set_index_service(**self.index_conf)
        pat_rec = dbs.save_patient(self.create_random_patient())
        arch_rec = ArchetypeInstance('openEHR-EHR-EVALUATION.dummy-evaluation.v1',
                                     {'field1': 'value1', 'field2': 'value2'})
        ehr_rec = ClinicalRecord(arch_rec)
        ehr_rec, pat_rec = dbs.save_ehr_record(ehr_rec, pat_rec)
        self.assertIsInstance(ehr_rec.record_id, str)
        self.assertTrue(ehr_rec.active)
        self.assertEqual(ehr_rec.ehr_data.archetype_class,
                         'openEHR-EHR-EVALUATION.dummy-evaluation.v1')
        self.assertEqual(ehr_rec.ehr_data.archetype_details,
                         {'field1': 'value1', 'field2': 'value2'})
        self.assertEqual(len(pat_rec.ehr_records), 1)
        self.assertEqual(pat_rec.ehr_records[0], ehr_rec)
        # cleanup
        dbs.delete_patient(pat_rec, cascade_delete=True)

    def test_save_ehr_records(self):
        dbs = DBServices(**self.conf)
        dbs.set_index_service(**self.index_conf)
        pat_rec = dbs.save_patient(self.create_random_patient())
        arch_rec = ArchetypeInstance('openEHR-EHR-EVALUATION.dummy-evaluation.v1',
                                     {'field1': 'value1', 'field2': 'value2'})
        ehr_recs = [ClinicalRecord(arch_rec) for x in xrange(0, 10)]
        ehr_recs, pat_rec, errors = dbs.save_ehr_records(ehr_recs, pat_rec)
        self.assertEqual(len(ehr_recs), 10)
        self.assertEqual(len(errors), 0)
        self.assertEqual(len(pat_rec.ehr_records), 10)
        self.assertEqual(pat_rec.ehr_records, ehr_recs)
        for e in ehr_recs:
            self.assertIsInstance(e.record_id, str)
            self.assertTrue(e.active)
            self.assertEqual(e.ehr_data.archetype_class,
                             'openEHR-EHR-EVALUATION.dummy-evaluation.v1')
            self.assertEqual(e.ehr_data.archetype_details,
                             {'field1': 'value1', 'field2': 'value2'})
        ehr_recs.extend([ClinicalRecord(arch_rec) for x in xrange(0, 5)])
        with self.assertRaises(DuplicatedKeyError) as ctx:
            dbs.save_ehr_records(ehr_recs, pat_rec)
        ehr_recs, pat_rec, errors = dbs.save_ehr_records(ehr_recs, pat_rec,
                                                         skip_existing_duplicated=True)
        self.assertEqual(len(ehr_recs), 5)
        self.assertEqual(len(errors), 10)
        self.assertEqual(len(pat_rec.ehr_records), 15)
        # cleanup
        dbs.delete_patient(pat_rec, cascade_delete=True)

    def test_remove_ehr_record(self):
        dbs = DBServices(**self.conf)
        dbs.set_index_service(**self.index_conf)
        pat_rec_1 = dbs.save_patient(self.create_random_patient())
        arch_rec = ArchetypeInstance('openEHR-EHR-EVALUATION.dummy-evaluation.v1',
                                 {'field1': 'value1', 'field2': 'value2'})
        ehr_rec = ClinicalRecord(arch_rec)
        ehr_rec, pat_rec_1 = dbs.save_ehr_record(ehr_rec, pat_rec_1)
        self.assertIsNotNone(ehr_rec.record_id)
        self.assertEqual(len(pat_rec_1.ehr_records), 1)
        old_ehr_id = ehr_rec.record_id
        ehr_rec, pat_rec_1 = dbs.remove_ehr_record(ehr_rec, pat_rec_1)
        self.assertNotEqual(old_ehr_id, ehr_rec.record_id)
        self.assertEqual(len(pat_rec_1.ehr_records), 0)
        pat_rec_2 = dbs.save_patient(self.create_random_patient())
        ehr_rec, pat_rec_2 = dbs.save_ehr_record(ehr_rec, pat_rec_2)
        self.assertEqual(len(pat_rec_2.ehr_records), 1)
        # cleanup
        dbs.delete_patient(pat_rec_1)
        dbs.delete_patient(pat_rec_2, cascade_delete=True)

    def test_load_ehr_records(self):
        dbs = DBServices(**self.conf)
        dbs.set_index_service(**self.index_conf)
        pat_rec = dbs.save_patient(PatientRecord(record_id='PATIENT_01'))
        for x in xrange(10):
            arch = ArchetypeInstance('openEHR-EHR-EVALUATION.dummy-evaluation.v1',
                                     {'ehr_field': 'ehr_value%02d' % x})
            _, pat_rec = dbs.save_ehr_record(ClinicalRecord(arch), pat_rec)
        pat_rec = dbs.get_patient('PATIENT_01', fetch_ehr_records=False)
        self.assertEqual(len(pat_rec.ehr_records), 10)
        for ehr in pat_rec.ehr_records:
            self.assertIsInstance(ehr.ehr_data, ArchetypeInstance)
            self.assertEqual(len(ehr.ehr_data.archetype_details), 0)
        pat_rec = dbs.load_ehr_records(pat_rec)
        self.assertEqual(len(pat_rec.ehr_records), 10)
        for ehr in pat_rec.ehr_records:
            self.assertIsInstance(ehr.ehr_data, ArchetypeInstance)
            self.assertNotEqual(len(ehr.ehr_data.archetype_details), 0)
        # cleanup
        dbs.delete_patient(pat_rec, cascade_delete=True)

    def _get_active_records_count(self, patient_record, counter):
        for ehr in patient_record.ehr_records:
            if ehr.active:
                counter['active'] += 1
            else:
                counter['hidden'] += 1
        return counter

    def test_hide_patient(self):
        dbs = DBServices(**self.conf)
        dbs.set_index_service(**self.index_conf)
        pat_rec = dbs.save_patient(self.create_random_patient())
        for x in xrange(10):
            arch = ArchetypeInstance('openEHR-EHR-EVALUATION.dummy-evaluation.v1',
                                     {'ehr_field': 'ehr_value%02d' % x})
            _, pat_rec = dbs.save_ehr_record(ClinicalRecord(arch), pat_rec)
        active_records = self._get_active_records_count(pat_rec, Counter())
        self.assertEqual(active_records['active'], 10)
        self.assertNotIn('hidden', active_records)
        pat_rec = dbs.hide_patient(pat_rec)
        self.assertFalse(pat_rec.active)
        active_records = self._get_active_records_count(pat_rec, Counter())
        self.assertEqual(active_records['hidden'], 10)
        self.assertNotIn('active', active_records)
        # cleanup
        dbs.delete_patient(pat_rec, cascade_delete=True)

    def test_hide_ehr_record(self):
        dbs = DBServices(**self.conf)
        dbs.set_index_service(**self.index_conf)
        pat_rec = dbs.save_patient(PatientRecord(record_id='PATIENT_01'))
        for x in xrange(20):
            arch = ArchetypeInstance('openEHR-EHR-EVALUATION.dummy-evaluation.v1',
                                     {'ehr_field': 'ehr_value%02d' % x})
            _, pat_rec = dbs.save_ehr_record(ClinicalRecord(arch), pat_rec)
        self.assertEqual(len(pat_rec.ehr_records), 20)
        active_records = self._get_active_records_count(pat_rec, Counter())
        self.assertEqual(active_records['active'], 20)
        self.assertNotIn('hidden', active_records)
        for ehr in pat_rec.ehr_records[5:15]:
            rec = dbs.hide_ehr_record(ehr)
        pat_rec = dbs.get_patient('PATIENT_01')
        self.assertEqual(len(pat_rec.ehr_records), 10)
        active_records = self._get_active_records_count(pat_rec, Counter())
        self.assertEqual(active_records['active'], 10)
        self.assertNotIn('hidden', active_records)
        pat_rec = dbs.get_patient('PATIENT_01', fetch_hidden_ehr=True)
        self.assertEqual(len(pat_rec.ehr_records), 20)
        active_records = self._get_active_records_count(pat_rec, Counter())
        self.assertEqual(active_records['active'], 10)
        self.assertEqual(active_records['hidden'], 10)
        # cleanup
        dbs.delete_patient(pat_rec, cascade_delete=True)

    def test_move_ehr_record(self):
        dbs = DBServices(**self.conf)
        dbs.set_index_service(**self.index_conf)
        pat_rec_1 = dbs.save_patient(self.create_random_patient())
        pat_rec_2 = dbs.save_patient(self.create_random_patient())
        arch = ArchetypeInstance('openEHR-EHR-EVALUATION.dummy-evaluation.v1',
                                 {'k1': 'v1', 'k2': 'v2'})
        ehr_rec = ClinicalRecord(arch)
        ehr_rec, pat_rec_1 = dbs.save_ehr_record(ehr_rec, pat_rec_1)
        self.assertIn(ehr_rec, pat_rec_1.ehr_records)
        self.assertNotIn(ehr_rec, pat_rec_2.ehr_records)
        pat_rec_1, pat_rec_2 = dbs.move_ehr_record(pat_rec_1, pat_rec_2, ehr_rec)
        self.assertNotIn(ehr_rec, pat_rec_1.ehr_records)
        self.assertIn(ehr_rec, pat_rec_2.ehr_records)
        # cleanup
        dbs.delete_patient(pat_rec_1)
        dbs.delete_patient(pat_rec_2, cascade_delete=True)

    def test_get_ehr_record(self):
        dbs = DBServices(**self.conf)
        dbs.set_index_service(**self.index_conf)
        p = dbs.save_patient(self.create_random_patient())
        arch = ArchetypeInstance(
            'openEHR-EHR-EVALUATION.dummy-evaluation.v1',
            {'k1': 'v1', 'k2': 'v2', 'k3': 'v3'}
        )
        crec_id = uuid.uuid4().hex
        dbs.save_ehr_record(ClinicalRecord(arch, record_id=crec_id), p)
        # wrong ehr_record_id and patient_id
        e = dbs.get_ehr_record(uuid.uuid4().hex, uuid.uuid4().hex)
        self.assertIsNone(e)
        # right ehr_record_id but wrong patient_id
        e = dbs.get_ehr_record(crec_id, uuid.uuid4().hex)
        self.assertIsNone(e)
        # right ehr_record_id and patient_id
        e = dbs.get_ehr_record(crec_id, p.record_id)
        self.assertIsInstance(e, ClinicalRecord)
        # cleanup
        dbs.delete_patient(p, cascade_delete=True)


def suite():
    suite = unittest.TestSuite()
    suite.addTest(TestDBServices('test_save_patient'))
    suite.addTest(TestDBServices('test_save_ehr_record'))
    suite.addTest(TestDBServices('test_save_ehr_records'))
    suite.addTest(TestDBServices('test_remove_ehr_record'))
    suite.addTest(TestDBServices('test_load_ehr_records'))
    suite.addTest(TestDBServices('test_hide_ehr_record'))
    suite.addTest(TestDBServices('test_move_ehr_record'))
    suite.addTest(TestDBServices('test_get_ehr_record'))
    return suite

if __name__ == '__main__':
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite())