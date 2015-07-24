from pyehr.ehr.services.dbmanager.drivers.factory import DriversFactory
from pyehr.utils import get_logger
from pyehr.ehr.services.dbmanager.dbservices.index_service import IndexService
from pyehr.ehr.services.dbmanager.dbservices.wrappers import PatientRecord, ClinicalRecord
from pyehr.ehr.services.dbmanager.errors import CascadeDeleteError, RedundantUpdateError,\
    RecordRestoreUnnecessaryError, OperationNotAllowedError, ConfigurationError
from pyehr.ehr.services.dbmanager.dbservices.version_manager import VersionManager

from collections import Counter


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
    :ivar ehr_repository: (optional) repository where clinical records data are stored
    :ivar ehr_versioning_repository: (optional) repository where older versions of clinical
      records are stored
    :ivar logger: logger for the DBServices class, if no logger is provided a new one
      is created
    """

    def __init__(self, driver, host, database, versioning_database=None,
                 patients_repository=None, ehr_repository=None,
                 ehr_versioning_repository=None, port=None, user=None,
                 passwd=None, logger=None):
        self.driver = driver
        self.host = host
        self.database = database
        self.versioning_database = versioning_database
        self.patients_repository = patients_repository
        self.ehr_repository = ehr_repository
        self.ehr_versioning_repository = ehr_versioning_repository
        self.port = port
        self.user = user
        self.passwd = passwd
        self.index_service = None
        self.logger = logger or get_logger('db_services')
        self.version_manager = self._set_version_manager()

    def _get_drivers_factory(self, repository):
        return DriversFactory(
            driver=self.driver,
            host=self.host,
            database=self.database,
            repository=repository,
            port=self.port,
            user=self.user,
            passwd=self.passwd,
            index_service=self.index_service,
            logger=self.logger
        )

    def _set_version_manager(self):
        return VersionManager(
            driver=self.driver,
            host=self.host,
            database=self.database,
            versioning_database=self.versioning_database,
            ehr_repository=self.ehr_repository,
            ehr_versioning_repository=self.ehr_versioning_repository,
            index_service=self.index_service,
            port=self.port,
            user=self.user,
            passwd=self.passwd,
            logger=self.logger
        )

    def _check_index_service(self):
        if not self.index_service:
            raise ConfigurationError('Operation not allowed, missing IndexService')

    def set_index_service(self, url, database, user, passwd):
        """
        Add a :class:`IndexService` to the current :class:`DBService` that will be used
        to index clinical records
        :param url: the URL of the :class:`IndexService`
        :type url: str
        :param database: the database used to store the indices
        :type database: str
        :param user: the user to access the :class:`IndexService`
        :type user: str
        :param passwd: the password to access the :class:`IndexService`
        :type passwd: str
        """
        self.index_service = IndexService(database, url, user, passwd, self.logger)
        # update version manager as well
        self.version_manager = self._set_version_manager()

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

    def _set_structure_id(self, ehr_record):
        ehr_data = ehr_record.ehr_data.to_json()
        structure_id = self.index_service.get_structure_id(ehr_data)
        ehr_record.structure_id = structure_id

    def save_ehr_record(self, ehr_record, patient_record, record_moved=False):
        """
        Save a clinical record into the DB and link it to a patient record

        :param ehr_record: EHR record that is going to be saved
        :type ehr_record: :class:`ClinicalRecord`
        :param patient_record: the reference :class:`PatientRecord` for the EHR record that
          is going to be saved
        :type patient_record: :class:`PatientRecord`
        :param record_moved: if True, the record has been moved from another patient, if
          False the record should be considered as a new one
        :type record_moved: bool
        :return: the :class:`ClinicalRecord` and the :class:`PatientRecord`
        """
        self._check_index_service()
        drf = self._get_drivers_factory(self.ehr_repository)
        ehr_record.bind_to_patient(patient_record)
        if ehr_record.is_persistent:
            if record_moved:
                ehr_record = self.version_manager.update_field(ehr_record, 'patient_id',
                                                               ehr_record.patient_id, 'last_update')
            else:
                raise OperationNotAllowedError('An already mapped record can\'t be assigned to a patient')
        else:
            # saving a new record, this is the first revision
            ehr_record.increase_version()
            # calculate and set the structure ID for the given record
            self._set_structure_id(ehr_record)
            with drf.get_driver() as driver:
                try:
                    driver.add_record(driver.encode_record(ehr_record))
                except Exception, e:
                    # if a new structure was created, delete it (reference counter is 0)
                    self.index_service.check_structure_counter(ehr_record.structure_id)
                    raise e
                self.index_service.increase_structure_counter(ehr_record.structure_id)
        patient_record = self._add_ehr_record(patient_record, ehr_record)
        return ehr_record, patient_record

    def save_ehr_records(self, ehr_records, patient_record, skip_existing_duplicated=False):
        """
        Save a batch of clinical records into the DB and link them to a patient record

        :param ehr_records: EHR records that are going to be saved
        :type ehr_records: list of :class:`ClinicalRecord` objects
        :param patient_record: the reference :class:`PatientRecord` for the EHR record that
          is going to be saved
        :type patient_record: :class:`PatientRecord`
        :param skip_existing_duplicated: if True, continue with the save operation even if one
          or more DuplicatedKeyError occur, if False raise an error
        :type skip_existing_duplicated: bool
        :return: a list with the saved :class:`ClinicalRecord`, the updated :class:`PatientRecord`
          and a list containing any records that caused a duplicated key error
        """
        self._check_index_service()
        drf = self._get_drivers_factory(self.ehr_repository)
        with drf.get_driver() as driver:
            for r in ehr_records:
                # calculate and set the structure ID for the given record
                self._set_structure_id(r)
                r.bind_to_patient(patient_record)
                if not r.is_persistent:
                    r.increase_version()
            encoded_records = [driver.encode_record(r) for r in ehr_records]
            try:
                saved, errors = driver.add_records(encoded_records, skip_existing_duplicated)
            except Exception, exc:
                for ehr in ehr_records:
                    self.index_service.check_structure_counter(ehr.structure_id)
                raise exc
            errors = [driver.decode_record(e) for e in errors]
        saved_struct_counter = Counter()
        for rec in ehr_records:
            if rec.record_id in saved:
                saved_struct_counter[rec.structure_id] += 1
        error_struct_counter = set([rec.record_id for rec in errors])
        for struct, counter in saved_struct_counter.iteritems():
            self.index_service.increase_structure_counter(struct, counter)
        for struct in error_struct_counter:
            self.index_service.check_structure_counter(struct)
        saved_ehr_records = [ehr for ehr in ehr_records if ehr.record_id in saved]
        patient_record = self._add_ehr_records(patient_record, saved_ehr_records)
        return saved_ehr_records, patient_record, errors

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
        self._add_to_list(patient_record, 'ehr_records', ehr_record.record_id)
        patient_record.ehr_records.append(ehr_record)
        return patient_record

    def _add_ehr_records(self, patient_record, ehr_records):
        """
        Add a list of already saved :class:`ClinicalRecord`s to the given :class:`PatientRecord`

        :param patient_record: the reference :class:`PatientRecord`
        :type patient_record: :class:`PatientRecord`
        :param ehr_records: the existing :class:`ClinicalRecord`s that are going to be added to the
          patient record
        :type ehr_records: list
        :return: the updated :class:`PatientRecord`
        """
        self._extend_list(patient_record, 'ehr_records', [ehr.record_id for ehr in ehr_records])
        patient_record.ehr_records.extend(ehr_records)
        return patient_record

    def update_ehr_record(self, ehr_record):
        """
        Save the given *ehr_record* as an update of the one already existing in the database.
        Record's ID and version number will be used to match the proper object. Existing EHR
        will be overwritten by the given one.

        :param ehr_record: the :class:`ClinicalRecord` that will be saved as update
        :type ehr_record: :class:`ClinicalRecord`
        :return: the given *ehr_record* with proper *update_timestamp* and *version*
          fields
        :rtype: :class:`ClinicalRecord`
        """
        if not ehr_record.is_persistent:
            raise OperationNotAllowedError('Record %s is not mapped in the DB, unable to update' %
                                           ehr_record.record_id)
        return self.version_manager.update_record(ehr_record)

    def _check_unecessary_restore(self, ehr_record):
        if ehr_record.version == 1:
            raise RecordRestoreUnnecessaryError('Record %s already at original revision' %
                                               ehr_record.record_id)
        elif ehr_record.version == 0:
            raise OperationNotAllowedError('Record %s is not mapped in the DB, unable to restore' %
                                           ehr_record.record_id)

    def restore_ehr_version(self, ehr_record, version):
        """
        Restore a specific *version* for the given *ehr_record*. All saved revisions that
        are newer than *version* will be deleted from the :class:`VersionManager`.

        :param ehr_record: the :class:`ClinicalRecord` that will be restored
        :type ehr_record: :class:`ClinicalRecord`
        :param version: the version of the record that will replace the one saved
          within the DB
        :type version: int
        :return: the restored :class:`ClinicalRecord` and the count of the revisions
          that have been deleted
        """
        self._check_unecessary_restore(ehr_record)
        return self.version_manager.restore_revision(ehr_record.record_id, version)

    def restore_original_ehr(self, ehr_record):
        """
        Restore the original version of the given *ehr_record*. All revisions
        will be deleted from the :class:`VersionManager`.

        :param ehr_record: the :class:`ClinicalRecord` that will be restored
        :type ehr_record: :class:`ClinicalRecord`
        :return: the restored :class:`ClinicalRecord` and the count of the revisions
          that have been deleted
        """
        self._check_unecessary_restore(ehr_record)
        return self.version_manager.restore_original(ehr_record.record_id)

    def restore_previous_ehr_version(self, ehr_record):
        """
        Restore giver *ehr_record* to its previous revision.

        :param ehr_record: the :class:`ClinicalRecord` that will be restored
        :type ehr_record: :class:`ClinicalRecord`
        :return: the restored :class:`ClinicalRecord`
        """
        return self.restore_ehr_version(ehr_record, ehr_record.version-1)[0]

    def get_revision(self, ehr_record, version):
        """
        Get a specific *version* of the given *ehr_record*

        :param ehr_record: the :class:`ClinicalRecord` that will be used to retrieve the wanted *version*
        :type ehr_record: :class:`ClinicalRecord`
        :param version: the revision of the object that will be retrieved
        :type version: int
        :return: a :class:`ClinicalRecordRevision` object matching the selected *version* or None
          if no match is fuond
        :rtype: :class:`ClinicalRecordRevision` or None
        """
        return self.version_manager.get_revision(ehr_record.record_id, version)

    def get_revisions(self, ehr_record, reverse_ordering=False):
        """
        Get all revisions for the given *ehr_record* ordered from the older to the newer.
        If *reverse_ordering* is True, revisions will be ordered from the newer to the older.

        :param ehr_record: the :class:`ClinicalRecord` for which will be retrieved old revisions
        :type ehr_record: :class:`ClinicalRecord`
        :param reverse_ordering: if False (default) revisions will be ordered from the older to
          the newer; if True the opposite ordering will be applied (newer to older).
        :type reverse_ordering: bool
        :return: an ordered list with all the revisions for the given *ehr_record*
        :rtype: list
        """
        return self.version_manager.get_revisions(ehr_record.record_id, reverse_ordering)

    def move_ehr_record(self, src_patient, dest_patient, ehr_record, reset_ehr_record_history=False):
        """
        Move a saved :class:`ClinicalRecord` from a saved :class:`PatientRecord` to another one

        :param src_patient: the :class:`PatientRecord` related to the EHR record that is going to be
          moved
        :type src_patient: :class:`PatientRecord`
        :param dest_patient: the :class:`PatientRecord` which will be associated with the the EHR record
        :type dest_patient: :class:`PatientRecord`
        :param ehr_record: the :class:`ClinicalRecord` that is going to be moved
        :type ehr_record: :class:`ClinicalRecord`
        :param reset_ehr_record_history: if True, reset EHR record history and delete all revisions, if False
          keep record's history and keep trace of the move event. Default value is False.
        :type reset_ehr_record_history: bool
        :return: the two :class:`PatientRecord` mapping the proper association to the EHR record
        """
        ehr_record, src_patient = self.remove_ehr_record(ehr_record, src_patient,
                                                         reset_record=reset_ehr_record_history)
        ehr_record, dest_patient = self.save_ehr_record(ehr_record, dest_patient,
                                                        record_moved=True)
        return src_patient, dest_patient

    def remove_ehr_record(self, ehr_record, patient_record, reset_record=True):
        """
        Remove a :class:`ClinicalRecord` from a patient's records and delete
        it from the database if *reset_record* is True.

        :param ehr_record: the :class:`ClinicalRecord` that will be deleted
        :type ehr_record: :class:`ClinicalRecord`
        :param patient_record: the reference :class:`PatientRecord`
        :type patient_record: :class:`PatientRecord`
        :param reset_record: if True, reset ehr record (new ID and delete its revisions)
        :type reset_record: bool
        :return: the EHR record without an ID and the updated patient record
        :rtype: :class:`ClinicalRecord`, :class:`PatientRecord`
        """
        self._remove_from_list(patient_record, 'ehr_records', ehr_record.record_id)
        patient_record.ehr_records.pop(patient_record.ehr_records.index(ehr_record))
        if reset_record:
            self._delete_ehr_record(ehr_record, reset_record)
            ehr_record.reset()
        else:
            ehr_record.patient_id = None
        return ehr_record, patient_record

    def remove_ehr_records(self, ehr_records, patient_record, reset_records=True):
        """

        :param ehr_records:
        :param patient_record:
        :param reset_records:
        :return:
        """
        self._remove_from_list(patient_record, 'ehr_records', [ehr.record_id for ehr in ehr_records])
        for ehr in ehr_records:
            patient_record.ehr_records.pop(patient_record.ehr_records.index(ehr))
        if reset_records:
            self._delete_ehr_records(ehr_records, reset_records)
            for ehr in ehr_records:
                ehr.reset()
        else:
            for ehr in ehr_records:
                ehr.patient_id = None
        return ehr_records, patient_record

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
        Load the :class:`PatientRecord` that matches the given ID from the DB.

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

    def get_ehr_record(self, ehr_record_id, patient_id):
        """
        Load a `ClinicalRecord` that matches the given *ehr_record_id* and that belongs
        to the `PatientRecord` with ID *patient_id*. If no record with *ehr_record_id* is found or
        if record doesn't belong to *patient_id* None will be returned.

        :param ehr_record_id: the ID of the clinical record
        :param patient_id: the ID of the patient that the clinical record must belong to
        :return: a :class:`ClinicalRecord` object or None
        """
        drf = self._get_drivers_factory(self.ehr_repository)
        with drf.get_driver() as driver:
            try:
                ehr_record = driver.decode_record(driver.get_record_by_id(ehr_record_id))
            except TypeError:
                return None
            if ehr_record.patient_id != patient_id:
                return None
            else:
                return ehr_record

    def load_ehr_records(self, patient):
        """
        Load all :class:`ClinicalRecord` objects connected to the given :class:`PatientRecord` object

        :param patient: the patient record object
        :type patient: :class:`PatientRecord`
        :return: the :class:`PatientRecord` object with loaded :class:`ClinicalRecord`
        :type: :class:`PatientRecord`
        """
        drf = self._get_drivers_factory(self.ehr_repository)
        with drf.get_driver() as driver:
            ehr_records = [driver.get_record_by_id(ehr.record_id) for ehr in patient.ehr_records]
            patient.ehr_records = [driver.decode_record(ehr) for ehr in ehr_records]
        return patient

    def hide_patient(self, patient):
        """
        Hide a :class:`PatientRecord` object

        :param patient: the patient record that is going to be hidden
        :type patient: :class:`PatientRecord`
        :return: the patient record
        :rtype: :class:`PatientRecord`
        """
        if patient.active:
            for ehr_rec in patient.ehr_records:
                try:
                    self.hide_ehr_record(ehr_rec)
                except RedundantUpdateError:
                    # just ignore RedundantUpdateError, this means that the records
                    # is already hidden
                    self.logger.debug('Record %s is already hidden', ehr_rec.record_id)
                    pass
            rec = self._hide_record(patient)
        else:
            rec = patient
        return rec

    def hide_ehr_record(self, ehr_record):
        """
        Hide a :class:`ClinicalRecord` object

        :param ehr_record: the clinical record that is going to be hidden
        :type ehr_record: :class:`ClinicalRecord`
        :return: the clinical record
        :rtype: :class:`ClinicalRecord`
        """
        if ehr_record.active:
            rec = self._hide_record(ehr_record)
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
            self._delete_ehr_records(patient.ehr_records)
            drf = self._get_drivers_factory(self.patients_repository)
            with drf.get_driver() as driver:
                driver.delete_record(patient.record_id)
                return None

    def _delete_ehr_record(self, ehr_record, reset_history=True):
        drf = self._get_drivers_factory(self.ehr_repository)
        with drf.get_driver() as driver:
            driver.delete_record(ehr_record.record_id)
            self.index_service.decrease_structure_counter(ehr_record.structure_id)
        if reset_history:
            self.version_manager.remove_revisions(ehr_record.record_id)
        return None

    def _delete_ehr_records(self, ehr_records, reset_history=True):
        drf = self._get_drivers_factory(self.ehr_repository)
        with drf.get_driver() as driver:
            driver.delete_records_by_id([ehr.record_id for ehr in ehr_records])
            struct_id_counter = Counter()
            for rec in ehr_records:
                struct_id_counter[rec.structure_id] += 1
            for str_id, str_count in struct_id_counter.iteritems():
                self.index_service.decrease_structure_counter(str_id, str_count)
        if reset_history:
            for ehr in ehr_records:
                self.version_manager.remove_revisions(ehr.record_id)
        return None

    def _hide_record(self, record):
        if isinstance(record, ClinicalRecord):
            return self.version_manager.update_field(record, 'active', False, 'last_update')
        else:
            drf = self._get_drivers_factory(self.patients_repository)
            with drf.get_driver() as driver:
                last_update = driver.update_field(record.record_id, 'active', False, 'last_update')
            record.last_update = last_update
            record.active = False
            return record

    def _add_to_list(self, record, list_label, element):
        if isinstance(record, ClinicalRecord):
            return self.version_manager.add_to_list(record, list_label, element, 'last_update')
        else:
            drf = self._get_drivers_factory(self.patients_repository)
            with drf.get_driver() as driver:
                last_update = driver.add_to_list(record.record_id, list_label, element, 'last_update')
            record.last_update = last_update
            return record

    def _extend_list(self, record, list_label, elements):
        if isinstance(record, ClinicalRecord):
            return self.version_manager.extend_list(record, list_label, elements, 'last_update')
        else:
            drf = self._get_drivers_factory(self.patients_repository)
            with drf.get_driver() as driver:
                last_update = driver.extend_list(record.record_id, list_label, elements, 'last_update')
            record.last_update = last_update
            return record

    def _remove_from_list(self, record, list_label, element):
        if isinstance(record, ClinicalRecord):
            return self.version_manager.remove_from_list(record, list_label, element, 'last_update')
        elif isinstance(record, PatientRecord):
            drf = self._get_drivers_factory(self.patients_repository)
            with drf.get_driver() as driver:
                last_update = driver.remove_from_list(record.record_id, list_label, element, 'last_update')
            record.last_update = last_update
            return record
        else:
            raise ValueError('Unable to handle object of class %s' % type(record))
