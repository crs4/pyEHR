import unittest
from pyehr.ehr.services.dbmanager.drivers.elastic_search import ElasticSearchDriver


class TestElasticSearchDriver(unittest.TestCase):

    def __init__(self, label):
        super(TestElasticSearchDriver, self).__init__(label)

    def test_connection(self):
        driver = ElasticSearchDriver([{"host": "localhost", "port": 9200}], 'test_database','test_collection')
        driver.connect()
        self.assertTrue(driver.is_connected)
        driver.disconnect()
        self.assertFalse(driver.is_connected)
        # check also the context manager
        with ElasticSearchDriver([{"host": "localhost", "port": 9200}], 'test_database', 'test_collection') as driver:
            self.assertTrue(driver.is_connected)
        self.assertFalse(driver.is_connected)

    def test_select_collection(self):
        with ElasticSearchDriver([{"host": "localhost", "port": 9200}], 'test_database', 'test_collection_1') as driver:
            self.assertEqual(driver.collection, u'test_collection_1')
            driver.select_collection('test_collection_2')
            self.assertEqual(driver.collection, u'test_collection_2')

    def test_add_record(self):
        record_id = '1'
        record = {
            '_id': record_id,
            'field1': 'value1',
            'field2': 'value2'
        }
        with ElasticSearchDriver([{"host": "localhost", "port": 9200}], 'test_database', 'test_collection') as driver:
            r_id = driver.add_record(record)
            self.assertEqual(r_id, record_id)
            self.assertEqual(driver.count(), 1)
            # cleanup
            driver.delete_record(r_id)


    def test_add_records(self):
        record_ids = [str(x) for x in xrange(0, 10)]
        records = [{
            '_id': rid,
            'field1': 'value1',
            'field2': 'value2',
        } for rid in record_ids]
        with ElasticSearchDriver([{"host": "localhost", "port": 9200}], 'test_database', 'test_collection') as driver:
            saved_ids, errors = driver.add_records(records)
            for sid in saved_ids:
                self.assertIn(sid, record_ids)
            self.assertEqual(driver.count(), 10)
            # cleanup
            for sid in saved_ids:
                driver.delete_record(sid)

    def test_get_record_by_id(self):
        record = {
            '_id': '1',
            'field1': 'value1',
            'field2': 'value2'
        }
        with ElasticSearchDriver([{"host": "localhost", "port": 9200}], 'test_database', 'test_collection') as driver:
            record_id = driver.add_record(record)
            self.assertEqual(record_id, record['_id'])
            rec = driver.get_record_by_id(record_id)
            self.assertEqual(rec, record)
            # cleanup
            driver.delete_record(record_id)

    def test_get_records_by_value(self):
        records = [
            {'value': x, 'even': x % 2 == 0, '_id': str(x)}
            for x in xrange(0, 20)
        ]
        with ElasticSearchDriver([{"host": "localhost", "port": 9200}], 'test_database', 'test_collection') as driver:
            record_ids, _ = driver.add_records(records)
            even_recs = list(driver.get_records_by_value('even', True))
            self.assertEqual(len(even_recs), 10)
            self.assertEqual(driver.count(), 20)
            # cleanup
            for rid in record_ids:
                driver.delete_record(rid)

    def test_update_field(self):
        record = {
            'label': 'label',
            'field1': 'value1',
            'field2': 'value2'
        }
        with ElasticSearchDriver([{"host": "localhost", "port": 9200}], 'test_database', 'test_collection') as driver:
            rec_id = driver.add_record(record)
            self.assertEqual(driver.get_record_by_id(rec_id)['label'], record['label'])
            driver.update_field(rec_id,'label','new_label')
            self.assertEqual(driver.get_record_by_id(rec_id)['label'], 'new_label')
            # cleanup
            driver.delete_record(rec_id)


def suite():
    suite = unittest.TestSuite()
    suite.addTest(TestElasticSearchDriver('test_connection'))
    suite.addTest(TestElasticSearchDriver('test_select_collection'))
    suite.addTest(TestElasticSearchDriver('test_add_record'))
    suite.addTest(TestElasticSearchDriver('test_add_records'))
    suite.addTest(TestElasticSearchDriver('test_get_record_by_id'))
    suite.addTest(TestElasticSearchDriver('test_get_records_by_value'))
    suite.addTest(TestElasticSearchDriver('test_update_field'))
    return suite

if __name__ == '__main__':
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite())