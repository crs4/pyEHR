from openehr.ehr.services.dbmanager.drivers.factory import DriversFactory
from openehr.utils import get_logger
from openehr.ehr.services.dbmanager.dbservices.wrappers import PatientRecord, ClinicalRecord, \
    RecordsFactory
from openehr.ehr.services.dbmanager.errors import CascadeDeleteError
import time


class DBServices(object):

    def __init__(self, driver, host, database, patients_repository=None,
                 ehr_repository=None, port=None, user=None, passwd=None, logger=None):
        self.driver = driver
        self.host = host
        self.database = database
        self.patients_repository = patients_repository
        self.ehr_repository = ehr_repository
        self.port = port
        self.user = user
        self.passwd = passwd
        self.logger = logger or get_logger('db_services')

    def _get_drivers_factory(self, repository):
        return DriversFactory(self.driver, self.host, self.database, repository,
                              self.user, self.passwd, self.logger)

    def save_patient(self, patient_record):
        """
        >>> dbs = DBServices('mongodb', 'localhost', 'test_database', 'test_patients_coll', 'test_ehr_coll')
        >>> pat_rec = PatientRecord()
        >>> pat_rec = dbs.save_patient(pat_rec)
        >>> len(pat_rec.ehr_records) == 0
        True
        >>> pat_rec.record_id is None
        False
        >>> pat_rec.active
        True
        >>> dbs.delete_patient(pat_rec) #cleanup
        """
        drf = self._get_drivers_factory(self.patients_repository)
        with drf.get_driver() as driver:
            patient_record.record_id = driver.add_record(patient_record._to_document())
            return patient_record

    def save_ehr_record(self, ehr_record, patient_record):
        """
        >>> dbs = DBServices('mongodb', 'localhost', 'test_database', 'test_patients_coll', 'test_ehr_coll')
        >>> pat_rec = PatientRecord()
        >>> pat_rec = dbs.save_patient(pat_rec)
        >>> ehr_rec = ClinicalRecord({'field1': 'value1', 'field2': 'value2'})
        >>> ehr_rec, pat_rec = dbs.save_ehr_record(ehr_rec, pat_rec)
        >>> ehr_rec.record_id is None
        False
        >>> len(pat_rec.ehr_records)
        1
        >>> pat_rec.ehr_records[0].record_id == ehr_rec.record_id
        True
        >>> dbs.delete_patient(pat_rec, cascade_delete=True) #cleanup
        """
        drf = self._get_drivers_factory(self.ehr_repository)
        with drf.get_driver() as driver:
            ehr_record.record_id = driver.add_record(ehr_record._to_document())
        patient_record = self.add_ehr_record(patient_record, ehr_record)
        return ehr_record, patient_record

    def add_ehr_record(self, patient_record, ehr_record):
        self._add_to_list(patient_record, 'ehr_records', ehr_record.record_id,
                          self.patients_repository)
        return self.get_patient(patient_record.record_id)

    def remove_ehr_record(self, ehr_record, patient_record):
        """
        Remove an EHR record from a patient's records and delete it from the database.
        Returns the EHR record without an ID so it can be assigned to a new patient (if needed).

        >>> dbs = DBServices('mongodb', 'localhost', 'test_database', 'test_patients_coll', 'test_ehr_coll')
        >>> pat_rec_1 = dbs.save_patient(PatientRecord())
        >>> ehr_rec = ClinicalRecord({'field1': 'value1', 'field2': 'value2'})
        >>> ehr_rec.record_id is None
        True
        >>> ehr_rec, pat_rec_1 = dbs.save_ehr_record(ehr_rec, pat_rec_1)
        >>> ehr_rec.record_id is None
        False
        >>> len(pat_rec_1.ehr_records)
        1
        >>> ehr_rec, pat_rec_1 = dbs.remove_ehr_record(ehr_rec, pat_rec_1)
        >>> ehr_rec.record_id is None
        True
        >>> len(pat_rec_1.ehr_records)
        0
        >>> pat_rec_2 = dbs.save_patient(PatientRecord())
        >>> ehr_rec, pat_rec_2 = dbs.save_ehr_record(ehr_rec, pat_rec_2)
        >>> ehr_rec.record_id is None
        False
        >>> len(pat_rec_2.ehr_records)
        1
        >>> dbs.delete_patient(pat_rec_1) # cleanup
        >>> dbs.delete_patient(pat_rec_2, cascade_delete=True) #cleanup
        """
        self._remove_from_list(patient_record, 'ehr_records', ehr_record.record_id,
                               self.patients_repository)
        drf = self._get_drivers_factory(self.ehr_repository)
        with drf.get_driver() as driver:
            driver.delete_record(ehr_record.record_id)
        ehr_record.record_id = None
        return ehr_record, self.get_patient(patient_record.record_id)

    def _get_active_records(self, driver):
        return driver.get_records_by_value('active', True)

    def _fetch_patient_data_full(self, patient_doc, fetch_ehr_records=True,
                                 fetch_hidden_ehr=False):
        drf = self._get_drivers_factory(self.ehr_repository)
        with drf.get_driver() as driver:
            patient_record = RecordsFactory.create_patient_record(patient_doc)
            ehr_records = []
            for ehr_id in patient_record.ehr_records:
                ehr_doc = driver.get_record_by_id(ehr_id)
                if fetch_hidden_ehr or (not fetch_hidden_ehr and ehr_doc['active']):
                    self.logger.debug('fetch_hidden_ehr: %s --- ehr_doc[\'active\']: %s',
                                      fetch_hidden_ehr, ehr_doc['active'])
                    ehr_records.append(RecordsFactory.create_clinical_record(ehr_doc,
                                                                             not fetch_ehr_records))
                    self.logger.debug('ehr_records: %r', ehr_records)
                else:
                    self.logger.debug('Ignoring hidden EHR record %r', ehr_doc['_id'])
            patient_record.ehr_records = ehr_records
            return patient_record

    def get_patients(self, active_records_only=True, fetch_ehr_records=True,
                     fetch_hidden_ehr=False):
        drf = self._get_drivers_factory(self.patients_repository)
        with drf.get_driver() as driver:
            if not active_records_only:
                return [self._fetch_patient_data_full(r, fetch_ehr_records,
                                                      fetch_hidden_ehr) for r in driver.get_all_records()]
            else:
                return [self._fetch_patient_data_full(r) for r in self._get_active_records()]

    def get_patient(self, patient_id, fetch_ehr_records=True, fetch_hidden_ehr=False):
        drf = self._get_drivers_factory(self.patients_repository)
        with drf.get_driver() as driver:
            return self._fetch_patient_data_full(driver.get_record_by_id(patient_id),
                                                 fetch_ehr_records, fetch_hidden_ehr)

    def hide_patient(self, patient):
        """
        >>> dbs = DBServices('mongodb', 'localhost', 'test_database', 'test_patients_coll', 'test_ehr_coll')
        >>> pat_rec = dbs.save_patient(PatientRecord())
        >>> for x in xrange(5):
        ...   ehr_rec, pat_rec = dbs.save_ehr_record(ClinicalRecord({'ehr_field': 'ehr_value_%02d' % x}), pat_rec)
        >>> pat_rec.active
        True
        >>> from collections import Counter
        >>> ct = Counter()
        >>> for ehr in pat_rec.ehr_records:
        ...   ct[ehr.active] += 1
        >>> print ct
        Counter({True: 5})
        >>> pat_rec = dbs.hide_patient(pat_rec)
        >>> pat_rec.active
        False
        >>> ct = Counter()
        >>> for ehr in pat_rec.ehr_records:
        ...   ct[ehr.active] += 1
        >>> print ct
        Counter({False: 5})
        >>> dbs.delete_patient(pat_rec, cascade_delete=True) #cleanup
        """
        if patient.active:
            for ehr_rec in patient.ehr_records:
                self.hide_ehr_record(ehr_rec)
            rec = self._hide_record(patient, self.patients_repository)
        else:
            rec = patient
        return rec

    def hide_ehr_record(self, ehr_record):
        """
        >>> dbs = DBServices('mongodb', 'localhost', 'test_database', 'test_patients_coll', 'test_ehr_coll')
        >>> pat_rec = dbs.save_patient(PatientRecord())
        >>> for x in xrange(5):
        ...   ehr_rec, pat_rec = dbs.save_ehr_record(ClinicalRecord({'ehr_field': 'ehr_value_%02d' % x}), pat_rec)
        >>> len(pat_rec.ehr_records)
        5
        >>> from collections import Counter
        >>> ct = Counter()
        >>> for ehr in pat_rec.ehr_records:
        ...   ct[ehr.active] += 1
        >>> print ct
        Counter({True: 5})
        >>> for ehr in pat_rec.ehr_records[2:]:
        ...   rec = dbs.hide_ehr_record(ehr)
        >>> pat_rec = dbs.get_patient(pat_rec.record_id)
        >>> print len(pat_rec.ehr_records)
        2
        >>> pat_rec = dbs.get_patient(pat_rec.record_id, fetch_hidden_ehr=True)
        >>> print len(pat_rec.ehr_records)
        5
        >>> ct = Counter()
        >>> for ehr in pat_rec.ehr_records:
        ...   ct[ehr.active] += 1
        >>> print ct
        Counter({False: 3, True: 2})
        >>> dbs.delete_patient(pat_rec, cascade_delete=True) #cleanup
        """
        if ehr_record.active:
            rec = self._hide_record(ehr_record, self.ehr_repository)
        else:
            rec = ehr_record
        return rec

    def delete_patient(self, patient, cascade_delete=False):
        def reload_patient(patient):
            return self.get_patient(patient.record_id, fetch_ehr_records=False,
                                    fetch_hidden_ehr=True)
        patient = reload_patient(patient)
        if not cascade_delete and len(patient.ehr_records) > 0:
            raise CascadeDeleteError('Unable to delete patient record with ID %s, %d EHR records still connected',
                                     patient.record_id, len(patient.ehr_records))
        else:
            for ehr_record in patient.ehr_records:
                self.delete_ehr_record(ehr_record)
            drf = self._get_drivers_factory(self.patients_repository)
            with drf.get_driver() as driver:
                driver.delete_record(patient.record_id)
                return None

    def delete_ehr_record(self, ehr_record):
        drf = self._get_drivers_factory(self.ehr_repository)
        with drf.get_driver() as driver:
            driver.delete_record(ehr_record.record_id)
            return None

    def _update_record_timestamp(self, update_condition):
        last_update = time.time()
        update_condition.setdefault('$set', {})['last_update'] = last_update
        self.logger.debug('Update condition is %r', update_condition)
        return update_condition, last_update

    def _hide_record(self, record, repository):
        drf = self._get_drivers_factory(repository)
        with drf.get_driver() as driver:
            last_update = driver.update_field(record.record_id, 'active', False, 'last_update')
        record.active = False
        record.last_update = last_update
        return record

    def _add_to_list(self, record, list_label, element, repository):
        drf = self._get_drivers_factory(repository)
        with drf.get_driver() as driver:
            driver.add_to_list(record.record_id, list_label, element, 'last_update')

    def _remove_from_list(self, record, list_label, element, repository):
        drf = self._get_drivers_factory(repository)
        with drf.get_driver() as driver:
            driver.remove_from_list(record.record_id, list_label, element, 'last_update')