import unittest, os
from lxml import etree
from pyehr.ehr.services.dbmanager.dbservices.index_service import IndexService

CONF_FILE = os.getenv('SERVICE_CONFIG_FILE')


class TestIndexService(unittest.TestCase):

    def __init__(self, label):
        super(TestIndexService, self).__init__(label)

    def test_structure_simple(self):
        ehr_record = {
            'archetype_class': 'test-openehr-OBSERVATION.test01.v1',
            'archetype_details': {
                'data': {
                    'at0001': {
                        'archetype_class': 'test-openehr-OBSERVATION.test02.v1',
                        'archetype_details': {}
                    }
                }
            }
        }
        expected_structure = '<archetype class="test-openehr-OBSERVATION.test01.v1" path_from_parent="/">' + \
            '<archetype class="test-openehr-OBSERVATION.test02.v1" path_from_parent="/data[at0001]"/>' + \
            '</archetype>'
        ehr_structure = etree.tostring(IndexService.get_structure(ehr_record))
        self.assertEqual(ehr_structure, expected_structure)

    def test_structure_dict(self):
        ehr_record = {
            'archetype_class': 'test-openehr-OBSERVATION.test01.v1',
            'archetype_details': {
                'cluster': {
                    'at0001': {
                        'archetype_class': 'test-openehr-OBSERVATION.test02.v1',
                        'archetype_details': {}
                    },
                    'at0002': {
                        'archetype_class': 'test-openehr-OBSERVATION.test03.v1',
                        'archetype_details': {}
                    },
                    'at0003': 'foobar'
                }
            }
        }
        expected_structure = '<archetype class="test-openehr-OBSERVATION.test01.v1" path_from_parent="/">' + \
            '<archetype class="test-openehr-OBSERVATION.test02.v1" path_from_parent="/cluster[at0001]"/>' + \
            '<archetype class="test-openehr-OBSERVATION.test03.v1" path_from_parent="/cluster[at0002]"/>' + \
            '</archetype>'
        ehr_structure = etree.tostring(IndexService.get_structure(ehr_record))
        self.assertEqual(ehr_structure, expected_structure)

    def test_structure_list(self):
        ehr_record = {
            'archetype_class': 'test-openehr-OBSERVATION.test01.v1',
            'archetype_details': {
                'event': {
                    'at0001': [
                        {
                            'archetype_class': 'test-openehr-OBSERVATION.test02.v1',
                            'archetype_details': {}
                        },
                        {
                            'archetype_class': 'test-openehr-OBSERVATION.test03.v1',
                            'archetype_details': {}
                        }
                    ]
                }
            }
        }
        expected_structure = '<archetype class="test-openehr-OBSERVATION.test01.v1" path_from_parent="/">' + \
            '<archetype class="test-openehr-OBSERVATION.test02.v1" path_from_parent="/event[at0001]"/>' + \
            '<archetype class="test-openehr-OBSERVATION.test03.v1" path_from_parent="/event[at0001]"/>' + \
            '</archetype>'
        ehr_structure = etree.tostring(IndexService.get_structure(ehr_record))
        self.assertEqual(ehr_structure, expected_structure)

    def test_structure_sorting(self):
        ehr_record_1 = {
            'archetype_class': 'test-openehr-OBSERVATION.test01.v1',
            'archetype_details': {
                'data': {
                    'at0001': [
                        {
                            'archetype_class': 'test-openehr-OBSERVATION.test02.v1',
                            'archetype_details': {}
                        },
                        {
                            'archetype_class': 'test-openehr-OBSERVATION.test03.v1',
                            'archetype_details': {}
                        }
                    ],
                    'at0011': {
                        'archetype_class': 'test-openehr-OBSERVATION.test10.v1',
                        'archetype_details': {}
                    }
                }
            }
        }
        ehr_record_2 = {
            'archetype_class': 'test-openehr-OBSERVATION.test01.v1',
            'archetype_details': {
                'data': {
                    'at0011': {
                        'archetype_class': 'test-openehr-OBSERVATION.test10.v1',
                        'archetype_details': {
                            'value': 'dummy_text'
                        }
                    },
                    'at0001': [
                        {
                            'archetype_class': 'test-openehr-OBSERVATION.test03.v1',
                            'archetype_details': {}
                        },
                        {
                            'archetype_class': 'test-openehr-OBSERVATION.test02.v1',
                            'archetype_details': {}
                        }
                    ]
                }
            }
        }
        ehr_structure_1 = etree.tostring(IndexService.get_structure(ehr_record_1))
        ehr_structure_2 = etree.tostring(IndexService.get_structure(ehr_record_2))
        self.assertEqual(ehr_structure_1, ehr_structure_2)


def suite():
    suite = unittest.TestSuite()
    suite.addTest(TestIndexService('test_structure_simple'))
    suite.addTest(TestIndexService('test_structure_dict'))
    suite.addTest(TestIndexService('test_structure_list'))
    suite.addTest(TestIndexService('test_structure_sorting'))
    return suite

if __name__ == '__main__':
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite())