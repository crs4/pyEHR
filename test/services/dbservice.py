import os, unittest, httplib2, sys, json, time
from urllib import urlencode
from urlparse import urlparse
from socket import error as serror
from openehr.utils import decode_dict

SERVICE_HOST = os.getenv('DBSERVICE_HOST', 'localhost')
SERVICE_PORT = os.getenv('DBSERVICE_PORT', '8080')


class TestDBService(unittest.TestCase):

    def __init__(self, label):
        super(TestDBService, self).__init__(label)
        self.uri = 'http://%s:%s' % (SERVICE_HOST, SERVICE_PORT)
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

    def setUp(self):
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
            'ehr_data': {'k1': 'v1', 'k2': 'v2'}
        }
        ehr_add_request = self._build_add_ehr_request(patient_id='TEST_PATIENT',
                                                      ehr_record=ehr_record_details)
        r, c = self._send_request(add_ehr_path, ehr_add_request)
        self.assertEqual(r.status, 500)
        # fix EHR record details
        ehr_record_details['archetype'] = 'openEHR.TEST-EVALUATION.v1'
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
        self.assertEqual(c['RECORD']['archetype'],
                         ehr_record_details['archetype'])
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


def suite():
    suite = unittest.TestSuite()
    suite.addTest(TestDBService('test_add_patient'))
    suite.addTest(TestDBService('test_add_ehr_record'))
    return suite

if __name__ == '__main__':
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run((suite()))