import unittest
from openehr.ehr.services.dbmanager.drivers.mongo import MongoDriver
from uuid import uuid4


class TestMongoDBDriver(unittest.TestCase):

    def __init__(self, label):
        super(TestMongoDBDriver, self).__init__(label)

    def test_connection(self):
        driver = MongoDriver('localhost', 'test_database', 'test_collection')
        driver.connect()
        self.assertTrue(driver.is_connected)
        driver.disconnect()
        self.assertFalse(driver.is_connected)
        # check also the context manager
        with MongoDriver('localhost', 'test_database', 'test_collection') as driver:
            self.assertTrue(driver.is_connected)
        self.assertFalse(driver.is_connected)

    def test_select_collection(self):
        with MongoDriver('localhost', 'test_database', 'test_collection_1') as driver:
            self.assertEqual(driver.collection.name, u'test_collection_1')
            driver.select_collection('test_collection_2')
            self.assertEqual(driver.collection.name, u'test_collection_2')

    def test_add_record(self):
        record_id = uuid4().hex
        record = {
            '_id': record_id,
            'field1': 'value1',
            'field2': 'value2'
        }
        with MongoDriver('localhost', 'test_database', 'test_collection') as driver:
            r_id = driver.add_record(record)
            self.assertEqual(r_id, record_id)
            self.assertEqual(driver.documents_count, 1)
            # cleanup
            driver.delete_record(r_id)

    def test_add_records(self):
        record_ids = [uuid4().hex for x in xrange(0, 10)]
        records = [{
            '_id': rid,
            'field1': 'value1',
            'field2': 'value2',
        } for rid in record_ids]
        with MongoDriver('localhost', 'test_database', 'test_collection') as driver:
            saved_ids = driver.add_records(records)
            for sid in saved_ids:
                self.assertIn(sid, record_ids)
            self.assertEqual(driver.documents_count, 10)
            # cleanup
            for sid in saved_ids:
                driver.delete_record(sid)

    def test_get_record_by_id(self):
        record = {
            '_id': uuid4().hex,
            'field1': 'value1',
            'field2': 'value2'
        }
        with MongoDriver('localhost', 'test_database', 'test_collection') as driver:
            record_id = driver.add_record(record)
            self.assertEqual(record_id, record['_id'])
            rec = driver.get_record_by_id(record_id)
            self.assertEqual(rec, record)
            # cleanup
            driver.delete_record(record_id)

    def test_get_records_by_query(self):
        records = [
            {'value': x, 'even': x % 2 == 0, '_id': uuid4().hex}
            for x in xrange(0, 20)
        ]
        with MongoDriver('localhost', 'test_database', 'test_collection') as driver:
            record_ids = driver.add_records(records)
            even_recs = list(driver.get_records_by_query({'even': True}))
            self.assertEqual(len(even_recs), 10)
            self.assertEqual(driver.documents_count, 20)
            # cleanup
            for rid in record_ids:
                driver.delete_record(rid)

    def test_update_record(self):
        record = {
            'label': 'label',
            'field1': 'value1',
            'field2': 'value2'
        }
        with MongoDriver('localhost', 'test_database', 'test_collection') as driver:
            rec_id = driver.add_record(record)
            self.assertEqual(driver.get_record_by_id(rec_id)['label'], record['label'])
            driver._update_record(rec_id, {'$set': {'label': 'new_label'}})
            self.assertEqual(driver.get_record_by_id(rec_id)['label'], 'new_label')
            # cleanup
            driver.delete_record(rec_id)


def suite():
    suite = unittest.TestSuite()
    suite.addTest(TestMongoDBDriver('test_connection'))
    suite.addTest(TestMongoDBDriver('test_select_collection'))
    suite.addTest(TestMongoDBDriver('test_add_record'))
    suite.addTest(TestMongoDBDriver('test_add_records'))
    suite.addTest(TestMongoDBDriver('test_get_record_by_id'))
    suite.addTest(TestMongoDBDriver('test_get_records_by_query'))
    suite.addTest(TestMongoDBDriver('test_update_record'))
    return suite

if __name__ == '__main__':
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite())