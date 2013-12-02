from openehr.ehr.services.dbmanager.drivers.mongo import MongoDriver
from openehr.utils import get_logger
from openehr.ehr.services.dbmanager.dbservices.wrappers import PatientRecord, ClinicalRecord


class DBServices(object):

    def __init__(self, host, database, patients_collection,
                 ehr_collection, port=None, user=None,
                 passwd=None, logger=None):
        self.host = host
        self.database = database
        self.patients_collection = patients_collection
        self.ehr_collection = ehr_collection
        self.port = port
        self.user = user
        self.passwd = passwd
        self.logger = logger or get_logger('db_services')

    def save_patient_record(self, patient_record):
        """
        >>> dbs = DBServices('localhost', 'test_database', 'test_patients_coll', 'test_ehr_coll')
        >>> pat_rec = PatientRecord()
        >>> pat_rec = dbs.save_patient_record(pat_rec)
        >>> len(pat_rec.ehr_records) == 0
        True
        >>> pat_rec.record_id is None
        False
        >>> pat_rec.active
        True
        """
        with MongoDriver(self.host, self.database, self.patients_collection,
                         self.port, self.user, self.passwd, self.logger) as driver:
            if patient_record.record_id is None:
                patient_record.record_id = driver.add_record(patient_record._to_document())
                return patient_record
            else:
                #TODO update or error?
                pass

    def _get_active_records(self, driver):
        return driver.get_records_by_query({'active': True})

    def get_patients(self, active_records_only=True):
        with MongoDriver(self.host, self.database, self.patients_collection,
                         self.port, self.user, self.passwd, self.logger) as driver:
            if not active_records_only:
                return driver.get_all_records()
            else:
                return self._get_active_records()

    def get_patient(self, patient_id):
        with MongoDriver(self.host, self.database, self.patients_collection,
                         self.port, self.user, self.passwd, self.logger) as driver:
            return driver.get_record_by_id(patient_id)

