import unittest, sys, os, uuid, random, copy
from pyehr.ehr.services.dbmanager.dbservices import DBServices
from pyehr.ehr.services.dbmanager.dbservices.wrappers import PatientRecord,\
    ClinicalRecord, ClinicalRecordRevision, ArchetypeInstance
from pyehr.ehr.services.dbmanager.errors import OptimisticLockError,\
    RedundantUpdateError, MissingRevisionError, RecordRestoreUnnecessaryError,\
    OperationNotAllowedError
from pyehr.utils.services import get_service_configuration

CONF_FILE = os.getenv('SERVICE_CONFIG_FILE')


class TestVersionManager(unittest.TestCase):

    def __init__(self, label):
        super(TestVersionManager, self).__init__(label)
        self.dbs = None
        self.patient = None

    def _create_random_patient(self):
        self.patient = PatientRecord(record_id=uuid.uuid4().hex)

    def _create_random_clinical_record(self):
        arch = ArchetypeInstance('openEHR-EHR-OBSERVATION.dummy-observation.v1',
                                 {'data': {
                                     'at0001': random.randint(1, 99),
                                     'at0002': 'just a text field'
                                 }})
        return ClinicalRecord(arch)

    def _create_random_complex_archetype(self):
        arch1 = ArchetypeInstance('openEHR-EHR-OBSERVATION.dummy-observation.v1',
                                  {
                                      'data': {
                                          'at0001': random.randint(1, 99),
                                          'at0002': 'just a text field'
                                      }
                                  })
        arch2 = ArchetypeInstance('openEHR-EHR-COMPOSITION.dummy-composition.v1',
                                  {
                                      'data': {
                                          'at1001': random.randint(1, 100),
                                          'at1002': arch1
                                      }
                                  })
        return arch2

    def build_dataset(self):
        self._create_random_patient()
        crec = self._create_random_clinical_record()
        self.patient = self.dbs.save_patient(self.patient)
        crec, self.patient = self.dbs.save_ehr_record(crec, self.patient)
        return crec

    def setUp(self):
        if CONF_FILE is None:
            sys.exit('ERROR: no configuration file provided')
        sconf = get_service_configuration(CONF_FILE)
        self.dbs = DBServices(**sconf.get_db_configuration())
        self.dbs.set_index_service(**sconf.get_index_configuration())

    def tearDown(self):
        if self.patient:
            self.dbs.delete_patient(self.patient, cascade_delete=True)
        self.dbs = None

    def test_record_update(self):
        crec = self.build_dataset()
        crec.ehr_data.archetype_details['data']['at0001'] = random.randint(100, 200)
        crec = self.dbs.update_ehr_record(crec)
        self.assertEqual(crec.version, 2)
        self.assertGreater(crec.last_update, crec.creation_time)

    def test_record_restore(self):
        crec = self.build_dataset()
        for x in xrange(0, 10):
            crec.ehr_data.archetype_details['data']['at0001'] = random.randint(100*x, 200*x)
            crec = self.dbs.update_ehr_record(crec)
            if x == 4:
                v6_value = crec.ehr_data.archetype_details['data']['at0001']
                v6_last_update = crec.last_update
        self.assertEqual(crec.version, 11)
        crec, deleted_revisions = self.dbs.restore_ehr_version(crec, 6)
        self.assertEqual(deleted_revisions, 5)
        self.assertEqual(crec.version, 6)
        self.assertEqual(crec.ehr_data.archetype_details['data']['at0001'],
                         v6_value)
        self.assertEqual(crec.last_update, v6_last_update)

    def test_record_restore_original(self):
        crec = self.build_dataset()
        original_last_update = crec.last_update
        original_value = crec.ehr_data.archetype_details['data']['at0001']
        for x in xrange(0, 10):
            crec.ehr_data.archetype_details['data']['at0001'] = random.randint(100*x, 200*x)
            crec = self.dbs.update_ehr_record(crec)
        self.assertEqual(crec.version, 11)
        crec, deleted_revisions = self.dbs.restore_original_ehr(crec)
        self.assertEqual(deleted_revisions, 10)
        self.assertEqual(crec.version, 1)
        self.assertEqual(crec.last_update, original_last_update)
        self.assertEqual(crec.ehr_data.archetype_details['data']['at0001'],
                         original_value)

    def test_record_restore_previous_revision(self):
        crec = self.build_dataset()
        crec_rev_1 = crec.to_json()
        crec.ehr_data.archetype_details['data']['at0001'] = random.randint(100, 200)
        crec = self.dbs.update_ehr_record(crec)
        crec_rev_2 = crec.to_json()
        crec.ehr_data.archetype_details['data']['at0002'] = 'updated text message'
        crec = self.dbs.update_ehr_record(crec)
        self.assertEqual(crec.version, 3)
        crec = self.dbs.restore_previous_ehr_version(crec)
        self.assertEqual(crec.to_json(), crec_rev_2)
        crec = self.dbs.restore_previous_ehr_version(crec)
        self.assertEqual(crec.to_json(), crec_rev_1)

    def test_record_reindex(self):
        crec = self.build_dataset()
        crec_struct_id = crec.structure_id
        crec.ehr_data = self._create_random_complex_archetype()
        crec = self.dbs.update_ehr_record(crec)
        self.assertNotEqual(crec_struct_id, crec.structure_id)
        self.assertEqual(crec.version, 2)
        self.assertGreater(crec.last_update, crec.creation_time)
        self.assertEqual(crec.ehr_data.archetype_class, 'openEHR-EHR-COMPOSITION.dummy-composition.v1')
        crec_struct_id = crec.structure_id
        crec, deleted_revisions = self.dbs.restore_original_ehr(crec)
        self.assertNotEqual(crec.structure_id, crec_struct_id)
        self.assertEqual(crec.version, 1)
        self.assertEqual(crec.ehr_data.archetype_class, 'openEHR-EHR-OBSERVATION.dummy-observation.v1')

    def test_get_revision(self):
        crec = self.build_dataset()
        for x in xrange(0, 10):
            crec.ehr_data.archetype_details['data']['at0001'] = random.randint(100*x, 200*x)
            crec = self.dbs.update_ehr_record(crec)
            if x == 4:
                v6_value = crec.ehr_data.archetype_details['data']['at0001']
                v6_last_update = crec.last_update
        crec_v6 = self.dbs.get_revision(crec, 6)
        self.assertIsInstance(crec_v6, ClinicalRecordRevision)
        self.assertEqual(crec_v6.last_update, v6_last_update)
        self.assertEqual(crec_v6.ehr_data.archetype_details['data']['at0001'],
                         v6_value)

    def test_get_revisions(self):
        crec = self.build_dataset()
        for x in xrange(0, 10):
            crec.ehr_data.archetype_details['data']['at0001'] = random.randint(100*x, 200*x)
            crec = self.dbs.update_ehr_record(crec)
        revisions = self.dbs.get_revisions(crec)
        self.assertEqual(len(revisions), 10)
        for rev in revisions:
            self.assertIsInstance(rev, ClinicalRecordRevision)
        self.assertEqual(revisions[0].version, 1)
        self.assertEqual(revisions[-1].version, 10)
        revisions = self.dbs.get_revisions(crec, reverse_ordering=True)
        self.assertEqual(len(revisions), 10)
        for rev in revisions:
            self.assertIsInstance(rev, ClinicalRecordRevision)
        self.assertEqual(revisions[0].version, 10)
        self.assertEqual(revisions[-1].version, 1)

    def test_optimistic_lock_error(self):
        # first user creates a clinical record
        crec1 = self.build_dataset()
        # second user retrieve the same record (using copy to make things fast)
        crec2 = copy.copy(crec1)
        # first user update the record
        crec1.ehr_data.archetype_details['data']['at0001'] = random.randint(100, 200)
        self.dbs.update_ehr_record(crec1)
        # second user try to update the record, an OptimisticLockError is raised
        with self.assertRaises(OptimisticLockError) as ctx:
            crec2.ehr_data.archetype_details['data']['at0002'] = 'updated text message'
            self.dbs.update_ehr_record(crec2)

    def test_redundant_update_error(self):
        crec = self.build_dataset()
        # record unchanged, try to update anyway
        with self.assertRaises(RedundantUpdateError) as ctx:
            self.dbs.update_ehr_record(crec)

    def test_missing_revision_error(self):
        # first user creates a clinical record and updates it several times
        crec1 = self.build_dataset()
        for x in xrange(0, 10):
            crec1.ehr_data.archetype_details['data']['at0001'] = random.randint(100*x, 200*x)
            crec1 = self.dbs.update_ehr_record(crec1)
        # second user get the last version of the same record (using copy as shortcut)
        crec2 = copy.copy(crec1)
        # first user restore the original version of the record
        self.dbs.restore_original_ehr(crec1)
        # second user restores one previous version of the record but this will fail
        # because used version no longer exists
        with self.assertRaises(MissingRevisionError) as ctx:
            self.dbs.restore_ehr_version(crec2, 5)

    def test_record_restore_unnecessary_error(self):
        crec = self.build_dataset()
        with self.assertRaises(RecordRestoreUnnecessaryError) as ctx:
            self.dbs.restore_previous_ehr_version(crec)
            self.dbs.restore_original_ehr(crec)

    def test_operation_not_allowed_error(self):
        crec = self._create_random_clinical_record()
        with self.assertRaises(OperationNotAllowedError):
            crec.ehr_data.archetype_details['data']['at0002'] = 'updated text message'
            self.dbs.update_ehr_record(crec)
            self.dbs.restore_original_ehr(crec)


def suite():
    suite = unittest.TestSuite()
    suite.addTest(TestVersionManager('test_record_update'))
    suite.addTest(TestVersionManager('test_record_restore'))
    suite.addTest(TestVersionManager('test_record_restore_original'))
    suite.addTest(TestVersionManager('test_record_restore_previous_revision'))
    suite.addTest(TestVersionManager('test_record_reindex'))
    suite.addTest(TestVersionManager('test_get_revision'))
    suite.addTest(TestVersionManager('test_get_revisions'))
    suite.addTest(TestVersionManager('test_optimistic_lock_error'))
    suite.addTest(TestVersionManager('test_redundant_update_error'))
    suite.addTest(TestVersionManager('test_missing_revision_error'))
    suite.addTest(TestVersionManager('test_record_restore_unnecessary_error'))
    suite.addTest(TestVersionManager('test_operation_not_allowed_error'))
    return suite

if __name__ == '__main__':
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite())