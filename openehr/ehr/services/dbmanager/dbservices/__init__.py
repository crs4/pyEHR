from openehr.ehr.services.dbmanager.drivers.factory import DriversFactory
from openehr.utils import get_logger
from openehr.ehr.services.dbmanager.dbservices.wrappers import PatientRecord, ClinicalRecord, \
    RecordsFactory
from openehr.ehr.services.dbmanager.errors import CascadeDeleteError
import time


class DBServices(object):
    """
    This class exports all the services used to manipulate patients' and clinical data
    stored in the database. The DBServices class acts as middleware to the specific driver.

    :ivar driver: the type of driver that must be used
    :ivar host: hostname of the server running the DB
    :ivar user: (optional) username to access the DB
    :ivar passwd: (optional) password to access the DB
    :ivar port: (optional) port used to contact the DB
    :ivar database: the name of the database where data are stored
    :ivar patients_repository: (optional) repository where patients' data are stored
    :ivar ehr_repository: (optional) repository where ehr data are stored
    :ivar logger: logger for the DBServices class, if no logger is provided a new one
      is created
    """

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
        Save a patient record to the DB.

        :param patient_record: patient record that is going to be saved
        :type patient_record: :class:`PatientRecord`
        :return: a :class:`PatientRecord` object

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
        Save a clinical record into the DB and link it to a patient record

        :param ehr_record: EHR record that is going to be saved
        :type ehr_record: :class:`ClinicalRecord`
        :param patient_record: the reference :class:`PatientRecord` for the EHR record that
          is going to be saved
        :type patient_record: :class:`PatientRecord`
        :return: the :class:`ClinicalRecord` and the :class:`PatientRecord`

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
        """
        Add an already saved :class:`ClinicalRecord` to the given :class:`PatientRecord`

        :param patient_record: the reference :class:`PatientRecord`
        :type patient_record: :class:`PatientRecord`
        :param ehr_record: the existing :class:`ClinicalRecord` that is going to be added to the
          patient record
        :type ehr_record: :class:`ClinicalRecord`
        :return: the updated :class:`PatientRecord`
        """
        self._add_to_list(patient_record, 'ehr_records', ehr_record.record_id,
                          self.patients_repository)
        return self.get_patient(patient_record.record_id)

    def remove_ehr_record(self, ehr_record, patient_record):
        """
        Remove a :class:`ClinicalRecord` from a patient's records and delete
        it from the database.

        :param ehr_record: the :class:`ClinicalRecord` that will be deleted
        :type ehr_record: :class:`ClinicalRecord`
        :param patient_record: the reference :class:`PatientRecord`
        :type patient_record: :class:`PatientRecord`
        :return: the EHR record without an ID and the updated patient record
        :rtype: :class:`ClinicalRecord`, :class:`PatientRecord`

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
        """
        Get all patients from the DB.

        :param active_records_only: if True fetch only active patient records, if False get all
          patient records from the DB
        :type active_records_only: boolean
        :param fetch_ehr_records: if True fetch connected EHR records  as well, if False only EHR records'
          IDs will be retrieved
        :type fetch_ehr_records: boolean
        :param fetch_hidden_ehr: if False only fetch active EHR records, if True fetch all EHR records
          connected to the given patient record
        :type fetch_hidden_ehr: boolean
        :return: a list of :class:`PatientRecord` objects
        """
        drf = self._get_drivers_factory(self.patients_repository)
        with drf.get_driver() as driver:
            if not active_records_only:
                return [self._fetch_patient_data_full(r, fetch_ehr_records,
                                                      fetch_hidden_ehr) for r in driver.get_all_records()]
            else:
                return [self._fetch_patient_data_full(r) for r in self._get_active_records()]

    def get_patient(self, patient_id, fetch_ehr_records=True, fetch_hidden_ehr=False):
        """
        Load the `PatientRecord` that match the given ID from the DB.

        :param patient_id: the ID of the record
        :param fetch_ehr_records: if True fetch connected EHR records  as well, if False only EHR records'
          IDs will be retrieved
        :type fetch_ehr_records: boolean
        :param fetch_hidden_ehr: if False only fetch active EHR records, if True fetch all EHR records
          connected to the given patient record
        :type fetch_hidden_ehr: boolean
        :return: the :class:`PatientRecord` matching the given ID or None if no matching record was found
        """
        drf = self._get_drivers_factory(self.patients_repository)
        with drf.get_driver() as driver:
            return self._fetch_patient_data_full(driver.get_record_by_id(patient_id),
                                                 fetch_ehr_records, fetch_hidden_ehr)

    def load_ehr_records(self, patient):
        """
        Load all :class:`ClinicalRecord` objects connected to the given :class:`PatientRecord` object

        :param patient: the patient record object
        :type patient: :class:`PatientRecord`
        :return: the :class:`PatientRecord` object with loaded :class:`ClinicalRecord`
        :type: :class;`PatientRecord`

        >>> dbs = DBServices('mongodb', 'localhost', 'test_database', 'test_patients_coll', 'test_ehr_coll')
        >>> pat_rec = dbs.save_patient(PatientRecord(record_id='PATIENT_01'))
        >>> for x in xrange(5):
        ...   ehr_rec, pat_rec = dbs.save_ehr_record(ClinicalRecord({'ehr_field': 'ehr_value%02d' % x}), pat_rec)
        >>> pat_rec = dbs.get_patient('PATIENT_01', fetch_ehr_records=False)
        >>> for ehr in pat_rec.ehr_records:
        ...   print ehr.ehr_data is None
        True
        True
        True
        True
        True
        >>> pat_rec = dbs.load_ehr_records(pat_rec)
        >>> for ehr in pat_rec.ehr_records:
        ...   print ehr.ehr_data is None
        False
        False
        False
        False
        False
        >>> dbs.delete_patient(pat_rec, cascade_delete=True) #cleanup
        """
        drf = self._get_drivers_factory(self.ehr_repository)
        with drf.get_driver() as driver:
            ehr_records = [driver.get_record_by_id(ehr.record_id) for ehr in patient.ehr_records]
            patient.ehr_records = [RecordsFactory.create_clinical_record(ehr) for ehr in ehr_records]
        return patient

    def hide_patient(self, patient):
        """
        Hide a ;class:`PatientRecord` object

        :param patient: the patient record that is going to be hidden
        :type param: :class:`PatientRecord`
        :return: the patient record
        :rtype: :class:`PatientRecord`

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
        Hide a :class:`ClinicalRecord` object

        :param ehr_record: the clinical record that is going to be hidden
        :type ehr_record: ;class:`ClinicalRecord`
        :return: the clinical record
        :rtype: :class:`ClinicalRecord`

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
        """
        Delete a patient from the DB. A patient record can be deleted only if it has no clinical record connected
        or if the *cascade_delete* option is set to True. If clinical records are still connected and *cascade_delete*
        option is set to False, a :class:`CascadeDeleteError` exception will be thrown.

        :param patient: the patient record that is going to be deleted
        :type patient: :class:`PatientRecord`
        :param cascade_delete: if True connected `ClinicalRecord` objects will be deleted as well
        :type cascade_delete: boolean
        :raise: :class:`CascadeDeleteError` if a record with connected clinical record is going to be deleted and
          cascade_delete option is False
        """
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