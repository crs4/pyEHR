from pyehr.ehr.services.dbmanager.drivers.factory import DriversFactory
from pyehr.utils import get_logger
from pyehr.ehr.services.dbmanager.dbservices.wrappers import PatientRecord, ClinicalRecord
from pyehr.ehr.services.dbmanager.errors import CascadeDeleteError
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
        return DriversFactory(
            driver=self.driver,
            host=self.host,
            database=self.database,
            repository=repository,
            port=self.port,
            user=self.user,
            passwd=self.passwd,
            logger=self.logger
        )

    def save_patient(self, patient_record):
        """
        Save a patient record to the DB.

        :param patient_record: patient record that is going to be saved
        :type patient_record: :class:`PatientRecord`
        :return: a :class:`PatientRecord` object
        """
        drf = self._get_drivers_factory(self.patients_repository)
        with drf.get_driver() as driver:
            patient_record.record_id = driver.add_record(driver.encode_record(patient_record))
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
        """
        drf = self._get_drivers_factory(self.ehr_repository)
        with drf.get_driver() as driver:
            ehr_record.record_id = driver.add_record(driver.encode_record(ehr_record))
        patient_record = self._add_ehr_record(patient_record, ehr_record)
        return ehr_record, patient_record

    def _add_ehr_record(self, patient_record, ehr_record):
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
        patient_record.ehr_records.append(ehr_record)
        return patient_record

    def move_ehr_record(self, src_patient, dest_patient, ehr_record):
        """
        Move a saved :class:`ClinicalRecord` from a saved :class:`PatientRecord` to another one

        :param src_patient: the :class:`PatientRecord` related to the EHR record that is going to be
          moved
        :type src_patient: :class:`PatientRecord`
        :param dest_patient: the :class:`PatientRecord` to whome the EHR record will be associated
        :type dest_patient: :class:`PatientRecord`
        :param ehr_record: the :class:`ClinicalRecord` that is going to be moved
        :type ehr_record: :class:`ClinicalRecord`
        :return: the two :class:`PatientRecord` mapping the proper association to the EHR record
        """
        ehr_record, src_patient = self.remove_ehr_record(ehr_record, src_patient, new_record_id=False)
        ehr_record, dest_patient = self.save_ehr_record(ehr_record, dest_patient)
        return src_patient, dest_patient

    def remove_ehr_record(self, ehr_record, patient_record, new_record_id=True):
        """
        Remove a :class:`ClinicalRecord` from a patient's records and delete
        it from the database.

        :param ehr_record: the :class:`ClinicalRecord` that will be deleted
        :type ehr_record: :class:`ClinicalRecord`
        :param patient_record: the reference :class:`PatientRecord`
        :type patient_record: :class:`PatientRecord`
        :param new_record_id: if True, get a new random record ID for the ehr_record
        :type new_record_id: bool
        :return: the EHR record without an ID and the updated patient record
        :rtype: :class:`ClinicalRecord`, :class:`PatientRecord`
        """
        self._remove_from_list(patient_record, 'ehr_records', ehr_record.record_id,
                               self.patients_repository)
        drf = self._get_drivers_factory(self.ehr_repository)
        with drf.get_driver() as driver:
            driver.delete_record(ehr_record.record_id)
        patient_record.ehr_records.pop(patient_record.ehr_records.index(ehr_record))
        if new_record_id:
            ehr_record.new_record_id()
        return ehr_record, patient_record

    def _get_active_records(self, driver):
        return driver.get_records_by_value('active', True)

    def _fetch_patient_data_full(self, patient_doc, fetch_ehr_records=True,
                                 fetch_hidden_ehr=False):
        drf = self._get_drivers_factory(self.ehr_repository)
        with drf.get_driver() as driver:
            patient_record = driver.decode_record(patient_doc)
            ehr_records = []
            for ehr in patient_record.ehr_records:
                ehr_doc = driver.get_record_by_id(ehr.record_id)
                if fetch_hidden_ehr or (not fetch_hidden_ehr and ehr_doc['active']):
                    self.logger.debug('fetch_hidden_ehr: %s --- ehr_doc[\'active\']: %s',
                                      fetch_hidden_ehr, ehr_doc['active'])
                    ehr_records.append(driver.decode_record(ehr_doc, fetch_ehr_records))
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
                patient_records = driver.get_all_records()
            else:
                patient_records = self._get_active_records(driver)
        return [self._fetch_patient_data_full(r, fetch_ehr_records,
                                              fetch_hidden_ehr) for r in patient_records]

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
            patient_record = driver.get_record_by_id(patient_id)
            if not patient_record:
                return None
            return self._fetch_patient_data_full(patient_record, fetch_ehr_records,
                                                 fetch_hidden_ehr)

    def load_ehr_records(self, patient):
        """
        Load all :class:`ClinicalRecord` objects connected to the given :class:`PatientRecord` object

        :param patient: the patient record object
        :type patient: :class:`PatientRecord`
        :return: the :class:`PatientRecord` object with loaded :class:`ClinicalRecord`
        :type: :class;`PatientRecord`
        """
        drf = self._get_drivers_factory(self.ehr_repository)
        with drf.get_driver() as driver:
            ehr_records = [driver.get_record_by_id(ehr.record_id) for ehr in patient.ehr_records]
            patient.ehr_records = [driver.decode_record(ehr) for ehr in ehr_records]
        return patient

    def hide_patient(self, patient):
        """
        Hide a ;class:`PatientRecord` object

        :param patient: the patient record that is going to be hidden
        :type patient: :class:`PatientRecord`
        :return: the patient record
        :rtype: :class:`PatientRecord`
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