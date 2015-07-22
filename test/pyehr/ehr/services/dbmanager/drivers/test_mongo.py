import sys, argparse, unittest, os

from pyehr.ehr.services.dbmanager.dbservices import DBServices
from pyehr.utils.services import get_service_configuration
from pyehr.ehr.services.dbmanager.errors import DuplicatedKeyError
from uuid import uuid4


conf_file = os.getenv('SERVICE_CONFIG_FILE')


class TestMongoDBDriver(unittest.TestCase):

    def __init__(self, label):
        super(TestMongoDBDriver, self).__init__(label)

    def setUp(self):
        conf = get_service_configuration(conf_file)
        db_conf = conf.get_db_configuration()
        db_service = DBServices(**db_conf)
        self.drf = db_service._get_drivers_factory(db_service.ehr_repository)

    def test_connection(self):
        driver = self.drf.get_driver()
        driver.connect()
        self.assertTrue(driver.is_connected)
        driver.disconnect()
        self.assertFalse(driver.is_connected)
        # check also the context manager
        with self.drf.get_driver() as driver:
            self.assertTrue(driver.is_connected)
        self.assertFalse(driver.is_connected)

    def test_select_collection(self):
        with self.drf.get_driver() as driver:
            self.assertEqual(driver.collection.name, u'test_ehr')
            driver.select_collection('test_ehr_2')
            self.assertEqual(driver.collection.name, u'test_ehr_2')

    def test_add_record(self):
        record_id = uuid4().hex
        record = {
            '_id': record_id,
            'field1': 'value1',
            'field2': 'value2'
        }
        with self.drf.get_driver() as driver:
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
        with self.drf.get_driver() as driver:
            saved_ids, errors = driver.add_records(records)
            for sid in saved_ids:
                self.assertIn(sid, record_ids)
            self.assertEqual(driver.documents_count, 10)
            self.assertEqual(len(errors), 0)
            # check for failure when records with duplicated ID  are in the same batch
            records.append(records[0])
            with self.assertRaises(DuplicatedKeyError) as context:
                driver.add_records(records)
            records.pop(-1)
            # check for failure when at least one record with an already used ID is in this batch
            records_ids_2 = [uuid4().hex for x in xrange(0, 5)]
            records.extend([{
                '_id': rid,
                'field1': 'value1',
                'field2': 'value2',
            } for rid in records_ids_2])
            with self.assertRaises(DuplicatedKeyError) as context:
                driver.add_records(records)
            self.assertEqual(driver.documents_count, 10)
            # save only records without a duplicated ID
            saved_ids_2, errors = driver.add_records(records, skip_existing_duplicated=True)
            self.assertEqual(len(saved_ids_2), 5)
            self.assertEqual(len(errors), 10)
            self.assertEqual(driver.documents_count, 15)
            saved_ids.extend(saved_ids_2)
            # cleanup
            for sid in saved_ids:
                driver.delete_record(sid)

    def test_get_record_by_id(self):
        record = {
            '_id': uuid4().hex,
            'field1': 'value1',
            'field2': 'value2'
        }
        with self.drf.get_driver() as driver:
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
        with self.drf.get_driver() as driver:
            record_ids, _ = driver.add_records(records)
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
        with self.drf.get_driver() as driver:
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