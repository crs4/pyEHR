import unittest, time
from pyehr.ehr.services.dbmanager.dbservices.wrappers \
    import ClinicalRecord, PatientRecord, ArchetypeInstance
from pyehr.ehr.services.dbmanager.errors import InvalidJsonStructureError


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
                'version': 0,
                'creation_time': creation_time,
                'last_update': creation_time,
                'ehr_data': {
                    'archetype_class': 'openEHR.EHR-TEST-EVALUATION.v1',
                    'archetype_details': {
                        'field1': 'value1',
                        'field2': 'value2'
                    }
                }
            }]
        }

        arch = ArchetypeInstance(
            archetype_class='openEHR.EHR-TEST-EVALUATION.v1',
            archetype_details={'field1': 'value1', 'field2': 'value2'}
        )
        crec = ClinicalRecord(
            ehr_data=arch,
            record_id='5314b3a55c98900a8a3d1a2c',
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
        arch_json = {
            'archetype_class': 'openEHR.EHR-TEST-EVALUATION.v1',
            'archetype_details': {'field1': 'value1', 'field2': 'value2'}
        }
        crec_json = {
            'creation_time': creation_time,
            'record_id': '5314b3a55c98900a8a3d1a2c'
        }
        prec_json = {
            'creation_time': creation_time,
            'record_id': 'V01245AC14CE2412412340'
        }
        with self.assertRaises(InvalidJsonStructureError) as context:
            ArchetypeInstance.from_json(arch_json)
            ClinicalRecord.from_json(crec_json)
            PatientRecord.from_json(prec_json)
        crec_json['ehr_data'] = arch_json
        prec_json['ehr_records'] = [crec_json]
        p = PatientRecord.from_json(prec_json)
        self.assertIsInstance(p, PatientRecord)
        for c in p.ehr_records:
            self.assertIsInstance(c, ClinicalRecord)
            self.assertIsInstance(c.record_id, str)
            self.assertIsInstance(c.ehr_data, ArchetypeInstance)

    def test_get_clinical_record(self):
        arch = ArchetypeInstance(
            archetype_class='openEHR-EHR-EVALUATION.dummy-evaluation.v1',
            archetype_details={'field1': 'value1', 'field2': 'value2'},
        )
        crec = ClinicalRecord(
            ehr_data=arch,
            record_id='5314b3a55c98931a8a3d1a2c'
        )
        prec = PatientRecord(
            ehr_records=[crec],
            record_id='V01245AC14CE2412412340'
        )
        self.assertIsNone(prec.get_clinical_record_by_id('FOOBAR'))
        self.assertIsInstance(prec.get_clinical_record_by_id('5314b3a55c98931a8a3d1a2c'),
                              ClinicalRecord)
        self.assertIsInstance(prec.get_clinical_record_by_id('5314b3a55c98931a8a3d1a2c'),
                              ClinicalRecord)

    def test_equal_records(self):
        def build_clinical_record(record_id=None):
            arch = ArchetypeInstance(
                archetype_class='openEHR-EHR-EVALUATION.dummy-evaluation.v1',
                archetype_details={'field1': 'value1', 'field2': 'value2'}
            )
            return ClinicalRecord(record_id=record_id,
                                  ehr_data=arch)
        prec1 = PatientRecord(record_id='PATIENT_1')
        prec2 = PatientRecord(record_id='PATIENT_2')
        self.assertNotEqual(prec1, prec2)
        self.assertEqual(prec1, PatientRecord(record_id='PATIENT_1'))
        crec1 = build_clinical_record()
        crec2 = build_clinical_record()
        self.assertNotEqual(crec1, crec2)


def suite():
    suite = unittest.TestSuite()
    suite.addTest(TestWrapper('test_to_json'))
    suite.addTest(TestWrapper('test_from_json'))
    suite.addTest(TestWrapper('test_get_clinical_record'))
    suite.addTest(TestWrapper('test_equal_records'))
    return suite

if __name__ == '__main__':
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite())