import os, unittest, httplib2, sys, json, time
from urllib import urlencode
from urlparse import urlparse
from socket import error as serror
from pyehr.utils import decode_dict
from pyehr.utils.services import get_service_configuration

CONF_FILE = os.getenv('SERVICE_CONFIG_FILE')


class TestDBService(unittest.TestCase):

    def __init__(self, label):
        super(TestDBService, self).__init__(label)
        self.method = 'POST'
        self.http = httplib2.Http()

    def _send_request(self, path, request_content):
        target = urlparse(self.uri + path)
        return self.http.request(target.geturl(), self.method,
                                 urlencode(request_content))

    def _build_add_patient_request(self, patient_id='JOHN_DOE', active=None,
                                   creation_time=None):
        request = {'patient_id': patient_id}
        if not active is None:
            request['active'] = active
        if creation_time:
            request['creation_time'] = creation_time
        return request

    def _build_add_ehr_request(self, patient_id='JOHN_DOE', ehr_record=None):
        request = {'patient_id': patient_id}
        if ehr_record:
            request['ehr_record'] = json.dumps(ehr_record)
        return request

    def _build_delete_patient_request(self, patient_id='JOHN_DOE', cascade_delete=None):
        request = {'patient_id': patient_id}
        if cascade_delete:
            request['cascade_delete'] = cascade_delete
        return request

    def _build_delete_ehr_request(self, patient_id='JOHN_DOE', ehr_record_id=None):
        request = {'patient_id': patient_id}
        if ehr_record_id:
            request['ehr_record_id'] = ehr_record_id
        return request

    def _build_patient_batch_request(self, patient_batch):
        return {
            'patient_data': json.dumps(patient_batch)
        }

    def _build_patients_batch_request(self, patients_batch):
        return {
            'patients_data': json.dumps(patients_batch)
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

    def setUp(self):
        if CONF_FILE is None:
            sys.exit('ERROR: no configuration file provided')
        conf = get_service_configuration(CONF_FILE)
        self.uri = 'http://%s:%s' % (conf.get_service_configuration()['host'],
                                     conf.get_service_configuration()['port'])

        target = urlparse(self.uri)
        try:
            r, c = self.http.request(target.geturl(), method=self.method)
        except serror:
            sys.exit('Unable to connect to %s, can\'t run the test' % target.geturl())

    def test_add_patient(self):
        add_path = '/patient/add'
        delete_path = '/patient/delete'
        # check mandatory fields
        r, c = self._send_request(add_path, {})
        self.assertEqual(r.status, 400)
        # check full patient record creation
        creation_time = time.time()
        patient_details = self._build_add_patient_request(patient_id='TEST_PATIENT',
                                                          active=False,
                                                          creation_time=creation_time)
        r, c = self._send_request(add_path, patient_details)
        self.assertEqual(r.status, 200)
        c = decode_dict(json.loads(c))
        self.assertTrue(c['SUCCESS'])
        self.assertEqual(c['RECORD']['record_id'], patient_details['patient_id'])
        self.assertEqual(c['RECORD']['creation_time'],
                         round(patient_details['creation_time'], 2))
        self.assertFalse(c['RECORD']['active'])
        # check duplicated entry
        r, c = self._send_request(add_path, patient_details)
        self.assertEqual(r.status, 500)
        # delete record
        patient_details = self._build_delete_patient_request(patient_id='TEST_PATIENT')
        r, c = self._send_request(delete_path, patient_details)
        self.assertEqual(r.status, 200)
        c = decode_dict(json.loads(c))
        self.assertTrue(c['SUCCESS'])

    def test_add_ehr_record(self):
        add_patient_path = '/patient/add'
        add_ehr_path = '/ehr/add'
        delete_patient_path = '/patient/delete'
        delete_ehr_path = '/ehr/delete'
        # create a patient
        patient_details = self._build_add_patient_request(patient_id='TEST_PATIENT')
        r, c = self._send_request(add_patient_path, patient_details)
        self.assertEqual(r.status, 200)
        # EHR creatrion, check mandatory fields
        r, c = self._send_request(add_ehr_path, {})
        self.assertEqual(r.status, 400)
        ehr_add_request = self._build_add_ehr_request(patient_id='TEST_PATIENT')
        r, c = self._send_request(add_ehr_path, patient_details)
        self.assertEqual(r.status, 400)
        # check for bad EHR JSON document (i.e. missing mandatory field in JSON document)
        ehr_record_details = {
            'ehr_data': {
                'archetype_details': {
                    'k1': 'v1', 'k2': 'v2'
                }
            }
        }
        ehr_add_request = self._build_add_ehr_request(patient_id='TEST_PATIENT',
                                                      ehr_record=ehr_record_details)
        r, c = self._send_request(add_ehr_path, ehr_add_request)
        self.assertEqual(r.status, 500)
        # fix EHR record details
        ehr_record_details['ehr_data']['archetype_class'] = 'openEHR.TEST-EVALUATION.v1'
        ehr_record_details['creation_time'] = time.time()
        ehr_record_details['active'] = True
        # check for non existing patient ID
        ehr_add_request = self._build_add_ehr_request(ehr_record=ehr_record_details)
        r, c = self._send_request(add_ehr_path, ehr_add_request)
        self.assertEqual(r.status, 200)
        c = decode_dict(json.loads(c))
        self.assertFalse(c['SUCCESS'])
        # save EHR record
        ehr_add_request = self._build_add_ehr_request(patient_id='TEST_PATIENT',
                                                      ehr_record=ehr_record_details)
        r, c = self._send_request(add_ehr_path, ehr_add_request)
        self.assertEqual(r.status, 200)
        c = decode_dict(json.loads(c))
        self.assertTrue(c['SUCCESS'])
        self.assertEqual(c['RECORD']['ehr_data']['archetype_class'],
                         ehr_record_details['ehr_data']['archetype_class'])
        self.assertEqual(c['RECORD']['ehr_data'],
                         ehr_record_details['ehr_data'])
        self.assertTrue(c['RECORD']['active'])
        # delete EHR record
        delete_ehr_details = self._build_delete_ehr_request(patient_id=patient_details['patient_id'],
                                                            ehr_record_id=c['RECORD']['record_id'])
        r, c = self._send_request(delete_ehr_path, delete_ehr_details)
        self.assertEqual(r.status, 200)
        c = decode_dict(json.loads(c))
        self.assertTrue(c['SUCCESS'])
        # delete patient record
        delete_patient_details = self._build_delete_patient_request(patient_id=patient_details['patient_id'])
        r, c = self._send_request(delete_patient_path, delete_patient_details)
        self.assertEqual(r.status, 200)

    def test_delete_patient(self):
        add_patient_path = '/patient/add'
        add_ehr_path = '/ehr/add'
        delete_patient_path = '/patient/delete'
        #check for mandatory fields
        r, c = self._send_request(delete_patient_path, {})
        self.assertEqual(r.status, 400)
        # add a patient
        patient_details = self._build_add_patient_request(patient_id='TEST_PATIENT')
        r, c = self._send_request(add_patient_path, patient_details)
        self.assertEqual(r.status, 200)
        # try to delete a non existing patient
        patient_delete_request = self._build_delete_patient_request()
        r, c = self._send_request(delete_patient_path, patient_delete_request)
        self.assertEqual(r.status, 200)
        c = decode_dict(json.loads(c))
        self.assertFalse(c['SUCCESS'])
        # delete patient
        patient_delete_request = self._build_delete_patient_request(patient_id='TEST_PATIENT')
        r, c = self._send_request(delete_patient_path, patient_delete_request)
        self.assertEqual(r.status, 200)
        c = decode_dict(json.loads(c))
        self.assertTrue(c['SUCCESS'])
        # create the patient again ...
        r, c = self._send_request(add_patient_path, patient_details)
        self.assertEqual(r.status, 200)
        # ... and add an EHR record
        ehr_record = {
            'ehr_data': {
                'archetype_class': 'openEHR.TEST-EVALUATION.v1',
                'archetype_details': {'k1': 'v1', 'k2': 'v2'}
            }
        }
        ehr_add_request = self._build_add_ehr_request(patient_id='TEST_PATIENT',
                                                      ehr_record=ehr_record)
        r, c = self._send_request(add_ehr_path, ehr_add_request)
        self.assertEqual(r.status, 200)
        # try to delete patient record without the cascade flag enabled
        r, c = self._send_request(delete_patient_path, patient_delete_request)
        self.assertEqual(r.status, 500)
        # enable the cascade delete and retry
        patient_delete_request = self._build_delete_patient_request(patient_id='TEST_PATIENT',
                                                                    cascade_delete=True)
        r, c = self._send_request(delete_patient_path, patient_delete_request)
        self.assertEqual(r.status, 200)
        c = decode_dict(json.loads(c))
        self.assertTrue(c['SUCCESS'])

    def test_delete_ehr_record(self):
        add_patient_path = '/patient/add'
        add_ehr_path = '/ehr/add'
        delete_patient_path = '/patient/delete'
        delete_ehr_path = '/ehr/delete'
        # create patient
        patient_details = self._build_add_patient_request(patient_id='TEST_PATIENT')
        r, c = self._send_request(add_patient_path, patient_details)
        self.assertEqual(r.status, 200)
        # add an EHR record
        ehr_record = {
            'ehr_data': {
                'archetype_class': 'openEHR.TEST-EVALUATION.v1',
                'archetype_details': {'k1': 'v1', 'k2': 'v2'}
            }
        }
        ehr_add_request = self._build_add_ehr_request(patient_id='TEST_PATIENT',
                                                      ehr_record=ehr_record)
        r, c = self._send_request(add_ehr_path, ehr_add_request)
        self.assertEqual(r.status, 200)
        c = decode_dict(json.loads(c))
        # get EHR record ID (will be used to deletere the record itself)
        ehr_record_id = c['RECORD']['record_id']
        self.assertTrue(c['SUCCESS'])
        # check for mandatory fields
        r, c = self._send_request(delete_ehr_path, {})
        self.assertEqual(r.status, 400)
        ehr_delete_request = self._build_delete_ehr_request(patient_id='TEST_PATIENT')
        r, c = self._send_request(delete_ehr_path, ehr_delete_request)
        self.assertEqual(r.status, 400)
        # try to delete an EHR record connected to a non existing individual
        ehr_delete_request = self._build_delete_ehr_request(ehr_record_id=ehr_record_id)
        r, c = self._send_request(delete_ehr_path, ehr_delete_request)
        self.assertEqual(r.status, 200)
        c = decode_dict(json.loads(c))
        self.assertFalse(c['SUCCESS'])
        # now try to delete a non existing EHR record
        ehr_delete_request = self._build_delete_ehr_request(patient_id='TEST_PATIENT',
                                                            ehr_record_id='THIS_IS_NOT_AN_EHR_ID')
        r, c = self._send_request(delete_ehr_path, ehr_delete_request)
        self.assertEqual(r.status, 200)
        c = decode_dict(json.loads(c))
        self.assertFalse(c['SUCCESS'])
        # delete the EHR record
        ehr_delete_request = self._build_delete_ehr_request(patient_id='TEST_PATIENT',
                                                            ehr_record_id=ehr_record_id)
        r, c = self._send_request(delete_ehr_path, ehr_delete_request)
        self.assertEqual(r.status, 200)
        c = decode_dict(json.loads(c))
        self.assertTrue(c['SUCCESS'])
        # delete patient record
        patient_delete_request = self._build_delete_patient_request(patient_id='TEST_PATIENT',
                                                                    cascade_delete=True)
        r, c = self._send_request(delete_patient_path, patient_delete_request)
        self.assertTrue(r.status, 200)

    def test_patient_batch(self):
        add_patient_batch_path = '/batch/save/patient'
        delete_patient_path = '/patient/delete'
        # check for mandatory fields
        r, c = self._send_request(add_patient_batch_path, {})
        self.assertEqual(r.status, 400)
        # build a batch with a bad PATIENT RECORD JSON structure and try to save it
        patient_batch = self._build_patient_batch(patient_id='TEST_PATIENT',
                                                  bad_patient_json=True)
        batch_request = self._build_patient_batch_request(patient_batch)
        r, c = self._send_request(add_patient_batch_path, batch_request)
        self.assertEqual(r.status, 500)
        # build a batch with a bad EHR RECORD JSON structure and try to save it
        patient_batch = self._build_patient_batch(patient_id='TEST_PATIENT',
                                                  bad_ehr_json=True)
        batch_request = self._build_patient_batch_request(patient_batch)
        r, c = self._send_request(add_patient_batch_path, batch_request)
        self.assertEqual(r.status, 500)
        # build a batch with an EHR RECORD with duplicated ID and try to save it
        patient_batch = self._build_patient_batch(patient_id='TEST_PATIENT',
                                                  duplicated_ehr_id=True)
        batch_request = self._build_patient_batch_request(patient_batch)
        r, c = self._send_request(add_patient_batch_path, batch_request)
        self.assertEqual(r.status, 500)
        # build a good batch and save it
        patient_batch = self._build_patient_batch(patient_id='TEST_PATIENT',
                                                  ehr_records_count=4)
        batch_request = self._build_patient_batch_request(patient_batch)
        r, c = self._send_request(add_patient_batch_path, batch_request)
        self.assertEqual(r.status, 200)
        c = decode_dict(json.loads(c))
        self.assertTrue(c['SUCCESS'])
        # try to save the same batch again (expected duplicated ID error)
        r, c = self._send_request(add_patient_batch_path, batch_request)
        self.assertEqual(r.status, 500)
        # cleanup
        patient_delete_request = self._build_delete_patient_request(patient_id='TEST_PATIENT',
                                                                    cascade_delete=True)
        r, c = self._send_request(delete_patient_path, patient_delete_request)
        self.assertEqual(r.status, 200)

    def test_patients_batch(self):
        add_patients_batch_path = '/batch/save/patients'
        delete_patient_path = '/patient/delete'
        # check for mandatory fields
        r, c = self._send_request(add_patients_batch_path, {})
        self.assertEqual(r.status, 400)
        # build a set with mixed bad and good records
        patients_batch = list()
        # first record contains an EHR record with bad JSON structure
        patients_batch.append(self._build_patient_batch(patient_id='TEST_PATIENT_1',
                                                        bad_ehr_json=True))
        # second record is a good one
        patients_batch.append(self._build_patient_batch(patient_id='TEST_PATIENT_1',
                                                        ehr_records_count=6))
        #third record contains two EHR records with the same ID
        patients_batch.append(self._build_patient_batch(patient_id='TEST_PATIENT_2',
                                                        ehr_records_count=3,
                                                        duplicated_ehr_id=True))
        # fourth record has the same PATIENT_ID sa record 2
        patients_batch.append(self._build_patient_batch(patient_id='TEST_PATIENT_1'))
        # fifth record is the last one and is a good one, it has no EHR records
        patients_batch.append(self._build_patient_batch(patient_id='TEST_PATIENT_2',
                                                        ehr_records_count=0))
        batch_request = self._build_patients_batch_request(patients_batch)
        r, c = self._send_request(add_patients_batch_path, batch_request)
        self.assertEqual(r.status, 200)
        c = decode_dict(json.loads(c))
        self.assertEqual(len(c['ERRORS']), 3)
        self.assertEqual(len(c['SAVED']), 2)
        # cleanup
        for saved in c['SAVED']:
            patient_delete_request = self._build_delete_patient_request(saved['record_id'],
                                                                        cascade_delete=True)
            r, c = self._send_request(delete_patient_path, patient_delete_request)
            self.assertEqual(r.status, 200)


def suite():
    suite = unittest.TestSuite()
    suite.addTest(TestDBService('test_add_patient'))
    suite.addTest(TestDBService('test_add_ehr_record'))
    suite.addTest(TestDBService('test_delete_patient'))
    suite.addTest(TestDBService('test_delete_ehr_record'))
    suite.addTest(TestDBService('test_patient_batch'))
    suite.addTest(TestDBService('test_patients_batch'))
    return suite

if __name__ == '__main__':
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite())