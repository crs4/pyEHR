import os, unittest, sys, json, time
from bson import ObjectId
from openehr.ehr.services.dbmanager.dbservices.wrappers \
    import ClinicalRecord, PatientRecord
from openehr.ehr.services.dbmanager.errors import InvalidJsonStructureError


class TestWrapper(unittest.TestCase):

    def __init__(self, label):
        super(TestWrapper, self).__init__(label)

    def test_to_json(self):
        creation_time = time.time()

        expected_json = {
            'record_id': 'V01245AC14CE2412412340',
            'active': True,
            'creation_time': creation_time,
            'last_update': creation_time,
            'ehr_records': [{
                'record_id': '5314b3a55c98900a8a3d1a2c',
                'active': True,
                'creation_time': creation_time,
                'last_update': creation_time,
                'archetype': 'openEHR.EHR-TEST-EVALUATION.v1',
                'ehr_data': {
                    'field1': 'value1',
                    'field2': 'value2'
                }
            }]
        }

        crec = ClinicalRecord(
            ehr_data={'field1': 'value1', 'field2': 'value2'},
            archetype='openEHR.EHR-TEST-EVALUATION.v1',
            record_id=ObjectId('5314b3a55c98900a8a3d1a2c'),
            creation_time=creation_time
        )
        prec = PatientRecord(
            ehr_records=[crec],
            creation_time=creation_time,
            record_id='V01245AC14CE2412412340'
        )
        json_data = prec.to_json()

        self.assertEqual(json_data, expected_json)

    def test_from_json(self):
        creation_time = time.time()

        crec_json = {
            'creation_time': creation_time,
            'archetype': 'openEHR.EHR-TEST-EVALUATION.v1',
            'record_id': '5314b3a55c98900a8a3d1a2c'
        }
        prec_json = {
            'creation_time': creation_time,
            'record_id': 'V01245AC14CE2412412340'
        }
        with self.assertRaises(InvalidJsonStructureError) as context:
            ClinicalRecord.from_json(crec_json)
            PatientRecord.from_json(prec_json)
        crec_json['ehr_data'] = {'field1': 'value1', 'field2': 'value2'}
        prec_json['ehr_records'] = [crec_json]
        p = PatientRecord.from_json(prec_json)
        self.assertIsInstance(p, PatientRecord)
        for c in p.ehr_records:
            self.assertIsInstance(c, ClinicalRecord)
            self.assertIsInstance(c.record_id, ObjectId)

    def test_get_clinical_record(self):
        crec = ClinicalRecord(
            archetype='openEHR-EHR-EVALUATION.dummy-evaluation.v1',
            ehr_data={'field1': 'value1', 'field2': 'value2'},
            record_id=ObjectId('5314b3a55c98931a8a3d1a2c')
        )
        prec = PatientRecord(
            ehr_records=[crec],
            record_id='V01245AC14CE2412412340'
        )
        self.assertIsNone(prec.get_clinical_record_by_id('FOOBAR'))
        self.assertIsInstance(prec.get_clinical_record_by_id('5314b3a55c98931a8a3d1a2c'),
                              ClinicalRecord)
        self.assertIsInstance(prec.get_clinical_record_by_id(ObjectId('5314b3a55c98931a8a3d1a2c')),
                              ClinicalRecord)


def suite():
    suite = unittest.TestSuite()
    suite.addTest(TestWrapper.test_to_json())
    suite.addTest(TestWrapper.test_from_json())
    suite.addTest(TestWrapper.test_get_clinical_record())
    return suite

if __name__ == '__main__':
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite())