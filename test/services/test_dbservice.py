import os, unittest, requests, sys, time
from copy import copy
try:
    import simplejson as json
except ImportError:
    import json
from pyehr.utils.services import get_service_configuration

CONF_FILE = os.getenv('SERVICE_CONFIG_FILE')


class TestDBService(unittest.TestCase):

    def __init__(self, label):
        super(TestDBService, self).__init__(label)
        if CONF_FILE is None:
            sys.exit('ERROR: no configuration file provided')
        sconf = get_service_configuration(CONF_FILE)
        self.dbservice_uri = 'http://%s:%s' % (sconf.get_db_service_configuration()['host'],
                                               sconf.get_db_service_configuration()['port'])
        self.patient_paths = {
            'add': 'patient',
            'delete': 'patient'
        }
        self.ehr_paths = {
            'add': 'ehr',
            'delete': 'ehr',
            'get': 'ehr',
            'update': 'ehr/update'
        }
        self.batch_path = {
            'save_patient': 'batch/save/patient',
            'save_patients': 'batch/save/patients'
        }

    def _get_path(self, path, patient_id=None, ehr_record_id=None, action=None):
        path_params = [self.dbservice_uri, path]
        if patient_id:
            path_params.append(patient_id)
        if ehr_record_id:
            path_params.append(ehr_record_id)
        if action:
            path_params.append(action)
        return '/'.join(path_params)

    def _build_add_patient_request(self, patient_id='JOHN_DOE', active=None,
                                   creation_time=None):
        request = {'patient_id': patient_id}
        if active is not None:
            request['active'] = active
        if creation_time:
            request['creation_time'] = creation_time
        return request

    def _build_add_ehr_request(self, patient_id='JOHN_DOE', ehr_record=None, active=None,
                               creation_time=None, record_id=None):
        request = {'patient_id': patient_id}
        if ehr_record:
            request['ehr_record'] = json.dumps(ehr_record)
        if active:
            request['active'] = active
        if creation_time:
            request['creation_time'] = creation_time
        if record_id:
            request['record_id'] = record_id
        return request

    def _build_delete_patient_request(self, cascade_delete=None):
        request = {}
        if cascade_delete:
            request['cascade_delete'] = cascade_delete
        return request

    def _build_ehr_update_request(self, ehr_record):
        request = {
            'ehr_record': json.dumps(ehr_record)
        }
        return request

    def _build_patient_batch_request(self, patient_batch):
        return {
            'patient_data': patient_batch
        }

    def _build_patients_batch_request(self, patients_batch):
        return {
            'patients_data': patients_batch
        }

    def _build_patient_batch(self, patient_id='JOHN_DOE', bad_patient_json=False,
                             ehr_records_count=2, bad_ehr_json=False,
                             duplicated_ehr_id=False):
        """
        If bad_patient_json is True, build a patient record without the ehr_records field
        If bad_ehr_json is True, build a list with len = (ehr_records_count + 1) of EHR records
        where the last record has an invalid JSON format (missing ehr_data field)
        If duplicated_ehr_id is True, build an EHR record with the same ID of one of the other
        EHR records
        """
        from uuid import uuid4

        patient_record = {
            'record_id': patient_id,
            'creation_time': time.time(),
            'active': True
        }
        if bad_patient_json:
            return patient_record
        patient_record['ehr_records'] = []
        for x in xrange(ehr_records_count):
            patient_record['ehr_records'].append(
                {
                    'ehr_data': {
                        'archetype_class': 'openEHR.TEST-EVALUATION.v1',
                        'archetype_details': {'rec%d.k1' % x: 'v1', 'rec%d.k2' % x: 'v2'}
                    },
                    'creation_time': time.time(),
                    'active': True,
                    'record_id': uuid4().hex
                }
            )
        if bad_ehr_json:
            patient_record['ehr_records'].append(
                {
                    'creation_time': time.time(),
                    'active': True,
                    'record_id': uuid4().hex
                }
            )
        if duplicated_ehr_id and len(patient_record['ehr_records']) > 0:
            patient_record['ehr_records'].append(
                {
                    'creation_time': time.time(),
                    'active': True,
                    'record_id': patient_record['ehr_records'][0]['record_id'],
                    'ehr_data': {
                        'archetype_class': 'openEHR.TEST-EVALUATION-DUPLICATED.v1',
                        'archetype_details': {
                            'rec%d.k1' % len(patient_record['ehr_records']): 'v1',
                            'rec%d.k2' % len(patient_record['ehr_records']): 'v2'
                        }
                    }
                }
            )
        return patient_record

    def test_add_patient(self):
        # check mandatory fields
        results = requests.put(self._get_path(self.patient_paths['add']), json={})
        self.assertEqual(results.status_code, requests.codes.bad)
        # check full patient record creation
        creation_time = time.time()
        patient_details = self._build_add_patient_request(patient_id='TEST_PATIENT',
                                                          active=False,
                                                          creation_time=creation_time)
        results = requests.put(self._get_path(self.patient_paths['add']), json=patient_details)
        self.assertEqual(results.status_code, requests.codes.ok)
        self.assertTrue(results.json()['SUCCESS'])
        response_patient = results.json()['RECORD']
        self.assertEqual(response_patient['record_id'], patient_details['patient_id'])
        self.assertEqual(response_patient['creation_time'], patient_details['creation_time'])
        self.assertFalse(response_patient['active'])
        # check duplicated entry
        results = requests.put(self._get_path(self.patient_paths['add']), json=patient_details)
        self.assertEqual(results.status_code, requests.codes.server_error)
        # cleanup
        results = requests.delete(self._get_path(self.patient_paths['delete'], patient_id='TEST_PATIENT',
                                                 action='delete'),
                                  json=self._build_delete_patient_request())
        self.assertEqual(results.status_code, requests.codes.ok)
        self.assertTrue(results.json()['SUCCESS'])

    def test_add_ehr_record(self):
        # create a patient
        patient_details = self._build_add_patient_request(patient_id='TEST_PATIENT')
        results = requests.put(self._get_path(self.patient_paths['add']), json=patient_details)
        self.assertEqual(results.status_code, requests.codes.ok)
        # EHR creation, check for mandatory fields
        results = requests.put(self._get_path(self.ehr_paths['add']), json={})
        self.assertEqual(results.status_code, requests.codes.bad)
        results = requests.put(self._get_path(self.ehr_paths['add']),
                               json=self._build_add_ehr_request(patient_id='TEST_PATIENT'))
        self.assertEqual(results.status_code, requests.codes.bad)
        # check for bad EHR JSON document (i.e. missing mandatory field in JSON document)
        ehr_record_details = {
            'archetype_details': {
                'k1': 'v1', 'k2': 'v2'
            }
        }
        results = requests.put(self._get_path(self.ehr_paths['add']),
                               json=self._build_add_ehr_request(patient_id='TEST_PATIENT',
                                                                ehr_record=ehr_record_details))
        self.assertEqual(results.status_code, requests.codes.server_error)
        # fix EHR record details
        ehr_record_details['archetype_class'] = 'openEHR.TEST-EVALUATION.v1'
        results = requests.put(self._get_path(self.ehr_paths['add']),
                               json=self._build_add_ehr_request(patient_id='TEST_PATIENT',
                                                                ehr_record=ehr_record_details))
        self.assertEqual(results.status_code, requests.codes.ok)
        self.assertTrue(results.json()['SUCCESS'])
        self.assertEqual(results.json()['RECORD']['ehr_data']['archetype_class'],
                         ehr_record_details['archetype_class'])
        self.assertEqual(results.json()['RECORD']['ehr_data'], ehr_record_details)
        self.assertTrue(results.json()['RECORD']['active'])
        # check for non existing patient ID
        results = requests.put(self._get_path(self.ehr_paths['add']),
                               json=self._build_add_ehr_request(ehr_record=ehr_record_details))
        self.assertEqual(results.status_code, requests.codes.ok)
        self.assertFalse(results.json()['SUCCESS'])
        # cleanup
        results = requests.delete(self._get_path(self.patient_paths['delete'], patient_id='TEST_PATIENT',
                                                 action='delete'),
                                  json=self._build_delete_patient_request(cascade_delete=True))
        self.assertEqual(results.status_code, requests.codes.ok)

    def test_delete_patient(self):
        # try to delete a non existing patient
        results = requests.delete(self._get_path(self.patient_paths['delete'], patient_id='JOHN_DOE',
                                                 action='delete'),
                                  json=self._build_delete_patient_request())
        self.assertEqual(results.status_code, requests.codes.ok)
        self.assertFalse(results.json()['SUCCESS'])
        # add a patient
        results = requests.put(self._get_path(self.patient_paths['add']),
                               json=self._build_add_patient_request(patient_id='TEST_PATIENT'))
        self.assertEqual(results.status_code, requests.codes.ok)
        # delete patient
        results = requests.delete(self._get_path(self.patient_paths['delete'], patient_id='TEST_PATIENT',
                                                 action='delete'),
                                  json=self._build_delete_patient_request())
        self.assertEqual(results.status_code, requests.codes.ok)
        self.assertTrue(results.json()['SUCCESS'])
        # create patient data again
        results = requests.put(self._get_path(self.patient_paths['add']),
                               json=self._build_add_patient_request(patient_id='TEST_PATIENT'))
        self.assertEqual(results.status_code, requests.codes.ok)
        # ... and add an EHR record
        ehr_record = {
            'archetype_class': 'openEHR.TEST-EVALUATION.v1',
            'archetype_details': {'k1': 'v1', 'k2': 'v2'}
        }
        results = requests.put(self._get_path(self.ehr_paths['add']),
                               json=self._build_add_ehr_request(patient_id='TEST_PATIENT',
                                                                ehr_record=ehr_record))
        self.assertEqual(results.status_code, requests.codes.ok)
        # try to delete patient record without the cascade delete flag enabled
        results = requests.delete(self._get_path(self.patient_paths['delete'], patient_id='TEST_PATIENT',
                                                 action='delete'),
                                  json=self._build_delete_patient_request())
        self.assertEqual(results.status_code, requests.codes.server_error)
        # retry enabling cascade deletion
        results = requests.delete(self._get_path(self.patient_paths['delete'], patient_id='TEST_PATIENT',
                                                 action='delete'),
                                  json=self._build_delete_patient_request(cascade_delete=True))
        self.assertEqual(results.status_code, requests.codes.ok)
        self.assertTrue(results.json()['SUCCESS'])

    def test_delete_ehr_record(self):
        # create patient and EHR record
        results = requests.put(self._get_path(self.patient_paths['add']),
                               json=self._build_add_patient_request(patient_id='TEST_PATIENT'))
        self.assertEqual(results.status_code, requests.codes.ok)
        ehr_record = {
            'archetype_class': 'openEHR.TEST-EVALUATION.v1',
            'archetype_details': {'k1': 'v1', 'k2': 'v2'}
        }
        results = requests.put(self._get_path(self.ehr_paths['add']),
                               json=self._build_add_ehr_request(patient_id='TEST_PATIENT',
                                                                ehr_record=ehr_record))
        self.assertEqual(results.status_code, requests.codes.ok)
        # get EHR record ID (will be used to delete the record itself)
        ehr_record_id = str(results.json()['RECORD']['record_id'])
        # delete an EHR record by giving a wrong Patient ID
        results = requests.delete(self._get_path(self.ehr_paths['delete'], patient_id='JOHN_DOE',
                                                 ehr_record_id=ehr_record_id, action='delete'))
        self.assertEqual(results.status_code, requests.codes.ok)
        self.assertFalse(results.json()['SUCCESS'])
        # try to delete a non existing EHR record (wrong record ID)
        results = requests.delete(self._get_path(self.ehr_paths['delete'], patient_id='TEST_PATIENT',
                                                 ehr_record_id='DUMMY_RECORD', action='delete'))
        self.assertEqual(results.status_code, requests.codes.ok)
        self.assertFalse(results.json()['SUCCESS'])
        # delete EHR record
        results = requests.delete(self._get_path(self.ehr_paths['delete'], patient_id='TEST_PATIENT',
                                                 ehr_record_id=ehr_record_id, action='delete'))
        self.assertEqual(results.status_code, requests.codes.ok)
        self.assertTrue(results.json()['SUCCESS'])
        # cleanup
        results = requests.delete(self._get_path(self.patient_paths['delete'], patient_id='TEST_PATIENT',
                                                 action='delete'),
                                  json=self._build_delete_patient_request(cascade_delete=True))
        self.assertEqual(results.status_code, requests.codes.ok)

    def test_get_ehr_record(self):
        # create a patient and an EHR record
        results = requests.put(self._get_path(self.patient_paths['add']),
                               json=self._build_add_patient_request(patient_id='TEST_PATIENT'))
        self.assertEqual(results.status_code, requests.codes.ok)
        ehr_record = {
            'archetype_class': 'openEHR.TEST-EVALUATION.v1',
            'archetype_details': {'k1': 'v1', 'k2': 'v2'}
        }
        results = requests.put(self._get_path(self.ehr_paths['add']),
                               json=self._build_add_ehr_request(patient_id='TEST_PATIENT',
                                                                ehr_record=ehr_record))
        self.assertEqual(results.status_code, requests.codes.ok)
        ehr_record_id = str(results.json()['RECORD']['record_id'])
        # wrong patient ID, right EHR record ID
        results = requests.get(self._get_path(self.ehr_paths['get'], patient_id='JOHN_DOE',
                                              ehr_record_id=ehr_record_id))
        self.assertEqual(results.status_code, requests.codes.ok)
        self.assertIsNone(results.json()['RECORD'])
        # right patient ID, wrong EHR record ID
        results = requests.get(self._get_path(self.ehr_paths['get'], patient_id='TEST_PATIENT',
                                              ehr_record_id='DUMMY_ID'))
        self.assertEqual(results.status_code, requests.codes.ok)
        self.assertIsNone(results.json()['RECORD'])
        # test with proper patient and EHR record IDs
        results = requests.get(self._get_path(self.ehr_paths['get'], patient_id='TEST_PATIENT',
                                              ehr_record_id=ehr_record_id))
        self.assertEqual(results.status_code, requests.codes.ok)
        self.assertEqual(str(results.json()['RECORD']['record_id']), ehr_record_id)
        self.assertEqual(str(results.json()['RECORD']['patient_id']), 'TEST_PATIENT')
        # cleanup
        results = requests.delete(self._get_path(self.patient_paths['delete'], patient_id='TEST_PATIENT',
                                                 action='delete'),
                                  json=self._build_delete_patient_request(cascade_delete=True))
        self.assertEqual(results.status_code, requests.codes.ok)

    def test_update_ehr_record(self):
        # create a patient and an EHR record
        results = requests.put(self._get_path(self.patient_paths['add']),
                               json=self._build_add_patient_request(patient_id='TEST_PATIENT'))
        self.assertEqual(results.status_code, requests.codes.ok)
        ehr_record = {
            'archetype_class': 'openEHR-EHR-COMPOSITION.dummy_composition.v1',
            'archetype_details': {'at0001': 'val1', 'at0002': 'val2'}
        }
        results = requests.put(self._get_path(self.ehr_paths['add']),
                               json=self._build_add_ehr_request(patient_id='TEST_PATIENT',
                                                                ehr_record=ehr_record))
        self.assertEqual(results.status_code, requests.codes.ok)
        ehr_record = results.json()['RECORD']
        # redundant update error
        results = requests.post(self._get_path(self.ehr_paths['update']),
                                json=self._build_ehr_update_request(ehr_record))
        self.assertEqual(results.status_code, requests.codes.ok)
        self.assertFalse(results.json()['SUCCESS'])
        # good update
        ehr_record['ehr_data']['archetype_details']['at1003'] = {
            'archetype_class': 'openEHR-EHR-OBSERVATION.dummy_observation.v1',
            'archetype_details': {'at1001': 'val01', 'at1002': 'val02'}
        }
        results = requests.post(self._get_path(self.ehr_paths['update']),
                                json=self._build_ehr_update_request(ehr_record))
        self.assertEqual(results.status_code, requests.codes.ok)
        self.assertGreater(results.json()['RECORD']['version'], ehr_record['version'])
        self.assertGreater(results.json()['RECORD']['last_update'], ehr_record['last_update'])
        ehr_record = results.json()['RECORD']
        # optimistic lock error
        ehr_record_2 = copy(ehr_record)
        ehr_record_2['ehr_data']['archetype_details']['at0001'] = 'new_val1'
        ehr_record_2['ehr_data']['archetype_details']['at0002'] = 'new_val2'
        results = requests.post(self._get_path(self.ehr_paths['update']),
                                json=self._build_ehr_update_request(ehr_record_2))
        self.assertEqual(results.status_code, requests.codes.ok)
        ehr_record['ehr_data']['archetype_details']['at0001'] = 'new_new_val1'
        ehr_record['ehr_data']['archetype_details']['at0002'] = 'new_new_val2'
        results = requests.post(self._get_path(self.ehr_paths['update']),
                                json=self._build_ehr_update_request(ehr_record))
        self.assertEqual(results.status_code, requests.codes.ok)
        self.assertFalse(results.json()['SUCCESS'])
        # try to update a non persistent record (version = 0)
        ehr_record['version'] = 0
        results = requests.post(self._get_path(self.ehr_paths['update']),
                                json=self._build_ehr_update_request(ehr_record))
        self.assertEqual(results.status_code, requests.codes.server_error)
        # cleanup
        results = requests.delete(self._get_path(self.patient_paths['delete'], patient_id='TEST_PATIENT',
                                                 action='delete'),
                                  json=self._build_delete_patient_request(cascade_delete=True))
        self.assertEqual(results.status_code, requests.codes.ok)

    def test_patient_batch(self):
        # check for mandatory fields
        results = requests.put(self._get_path(self.batch_path['save_patient']), json={})
        self.assertEqual(results.status_code, requests.codes.bad)
        # build a batch with a bad PATIENT RECORD JSON structure and try to save it
        patient_batch = self._build_patient_batch(patient_id='TEST_PATIENT',
                                                  bad_patient_json=True)
        results = requests.put(self._get_path(self.batch_path['save_patient']),
                               json=self._build_patient_batch_request(patient_batch))
        self.assertEqual(results.status_code, requests.codes.server_error)
        # build a batch with a bad EHR RECORD JSON structure and try to save it
        patient_batch = self._build_patient_batch(patient_id='TEST_PATIENT',
                                                  bad_ehr_json=True)
        results = requests.put(self._get_path(self.batch_path['save_patient']),
                               json=self._build_patient_batch_request(patient_batch))
        self.assertEqual(results.status_code, requests.codes.server_error)
        # build a batch with an EHR RECORD with duplicated ID and try to save it
        patient_batch = self._build_patient_batch(patient_id='TEST_PATIENT',
                                                  duplicated_ehr_id=True)
        results = requests.put(self._get_path(self.batch_path['save_patient']),
                               json=self._build_patient_batch_request(patient_batch))
        self.assertEqual(results.status_code, requests.codes.server_error)
        # build a good batch and save it
        patient_batch = self._build_patient_batch(patient_id='TEST_PATIENT',
                                                  ehr_records_count=4)
        results = requests.put(self._get_path(self.batch_path['save_patient']),
                               json=self._build_patient_batch_request(patient_batch))
        self.assertEqual(results.status_code, requests.codes.ok)
        self.assertTrue(results.json()['SUCCESS'])
        # try to save the same batch again (expected duplicated ID error)
        results = requests.put(self._get_path(self.batch_path['save_patient']),
                               json=self._build_patient_batch_request(patient_batch))
        self.assertEqual(results.status_code, requests.codes.server_error)
        # cleanup
        results = requests.delete(self._get_path(self.patient_paths['delete'], patient_id='TEST_PATIENT',
                                                 action='delete'),
                                  json=self._build_delete_patient_request(cascade_delete=True))
        self.assertEqual(results.status_code, requests.codes.ok)

    def test_patients_batch(self):
        # check for mandatory fields
        results = requests.put(self._get_path(self.batch_path['save_patients']), json={})
        self.assertEqual(results.status_code, requests.codes.bad)
        # build a set with mixed bad and good records
        patients_batch = list()
        # first record contains an EHR record with bad JSON structure
        patients_batch.append(self._build_patient_batch(patient_id='TEST_PATIENT_1',
                                                        bad_ehr_json=True))
        # second record is a good one
        patients_batch.append(self._build_patient_batch(patient_id='TEST_PATIENT_1',
                                                        ehr_records_count=6))
        # third record contains two EHR records with the same ID
        patients_batch.append(self._build_patient_batch(patient_id='TEST_PATIENT_2',
                                                        ehr_records_count=3,
                                                        duplicated_ehr_id=True))
        # fourth record has the same PATIENT_ID as record 2
        patients_batch.append(self._build_patient_batch(patient_id='TEST_PATIENT_1'))
        # fifth record is the last one and is a good one, it has no EHR records
        patients_batch.append(self._build_patient_batch(patient_id='TEST_PATIENT_2',
                                                        ehr_records_count=0))
        results = requests.put(self._get_path(self.batch_path['save_patients']),
                               json=self._build_patients_batch_request(patients_batch))
        self.assertEqual(results.status_code, requests.codes.ok)
        self.assertEqual(len(results.json()['ERRORS']), 3)
        self.assertEqual(len(results.json()['SAVED']), 2)
        # cleanup
        for saved in results.json()['SAVED']:
            res = requests.delete(self._get_path(self.patient_paths['delete'],
                                                 patient_id=str(saved['record_id']),
                                                 action='delete'),
                                  json=self._build_delete_patient_request(cascade_delete=True))
            self.assertEqual(res.status_code, requests.codes.ok)


def suite():
    suite = unittest.TestSuite()
    suite.addTest(TestDBService('test_add_patient'))
    suite.addTest(TestDBService('test_add_ehr_record'))
    suite.addTest(TestDBService('test_delete_patient'))
    suite.addTest(TestDBService('test_delete_ehr_record'))
    suite.addTest(TestDBService('test_get_ehr_record'))
    suite.addTest(TestDBService('test_patient_batch'))
    suite.addTest(TestDBService('test_patients_batch'))
    return suite

if __name__ == '__main__':
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite())