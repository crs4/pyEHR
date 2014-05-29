import unittest, os
from lxml import etree
from pyehr.ehr.services.dbmanager.dbservices.index_service import IndexService
from pyehr.ehr.services.dbmanager.dbservices.wrappers import PatientRecord,\
    ClinicalRecord, ArchetypeInstance
from pyehr.utils.services import get_service_configuration

CONF_FILE = os.getenv('SERVICE_CONFIG_FILE')


class TestIndexService(unittest.TestCase):

    def __init__(self, label):
        super(TestIndexService, self).__init__(label)

    def test_structure_simple(self):
        ehr_record = {
            'archetype': 'test-openehr-OBSERVATION.test01.v1',
            'data': {
                'at0001': {
                    'archetype': 'test-openehr-OBSERVATION.test02.v1',
                    'data': {}
                }
            }
        }
        expected_structure = '<archetype class="test-openehr-OBSERVATION.test01.v1">' + \
            '<archetype class="test-openehr-OBSERVATION.test02.v1"/>' + \
            '</archetype>'
        ehr_structure = etree.tostring(IndexService.get_structure(ehr_record))
        self.assertEqual(ehr_structure, expected_structure)

    def test_structure_dict(self):
        ehr_record = {
            'archetype': 'test-openehr-OBSERVATION.test01.v1',
            'data': {
                'cluster': {
                    'at0001': {
                        'archetype': 'test-openehr-OBSERVATION.test02.v1',
                        'data': {}
                    },
                    'at0002': {
                        'archetype': 'test-openehr-OBSERVATION.test03.v1',
                        'data': {}
                    },
                    'at0003': 'foobar'
                }
            }
        }
        expected_structure = '<archetype class="test-openehr-OBSERVATION.test01.v1">' + \
            '<archetype class="test-openehr-OBSERVATION.test02.v1"/>' + \
            '<archetype class="test-openehr-OBSERVATION.test03.v1"/>' + \
            '</archetype>'
        ehr_structure = etree.tostring(IndexService.get_structure(ehr_record))
        self.assertEqual(ehr_structure, expected_structure)

    def test_structure_list(self):
        ehr_record = {
            'archetype': 'test-openehr-OBSERVATION.test01.v1',
            'data': {
                'at0001': [
                    {
                        'archetype': 'test-openehr-OBSERVATION.test02.v1',
                        'data': {}
                    },
                    {
                        'archetype': 'test-openehr-OBSERVATION.test03.v1',
                        'data': {}
                    }
                ]
            }
        }
        expected_structure = '<archetype class="test-openehr-OBSERVATION.test01.v1">' + \
            '<archetype class="test-openehr-OBSERVATION.test02.v1"/>' + \
            '<archetype class="test-openehr-OBSERVATION.test03.v1"/>' + \
            '</archetype>'
        ehr_structure = etree.tostring(IndexService.get_structure(ehr_record))
        self.assertEqual(ehr_structure, expected_structure)


def suite():
    suite = unittest.TestSuite()
    suite.addTest(TestIndexService('test_structure_simple'))
    suite.addTest(TestIndexService('test_structure_dict'))
    suite.addTest(TestIndexService('test_structure_list'))
    return suite

if __name__ == '__main__':
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite())