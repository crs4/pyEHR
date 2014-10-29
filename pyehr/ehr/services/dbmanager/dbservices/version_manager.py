from hashlib import md5
import json

from pyehr.ehr.services.dbmanager.drivers.factory import DriversFactory
from pyehr.utils import get_logger
from pyehr.ehr.services.dbmanager.errors import OptimisticLockError,\
    RedundantUpdateError, RecordRestoreFailedError


class VersionManager(object):
    """
    TODO: add documentation here
    """

    def __init__(self, driver, host, database, ehr_repository=None,
                 ehr_versioning_repository=None, port=None, user=None,
                 passwd=None, logger=None):
        self.driver = driver
        self.host = host
        self.database = database
        self.ehr_repository = ehr_repository
        self.ehr_versioning_repository = ehr_versioning_repository
        self.port = port
        self.user = user
        self.passwd = passwd
        self.logger = logger or get_logger('version_manager')

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

    def _check_redundant_update(self, new_record, old_record):
        new_record_hash = md5()
        new_record_hash.update(json.dumps(new_record.to_json()))
        old_record_hash = md5()
        old_record_hash.update(json.dumps(old_record.to_json()))
        if new_record_hash.hexdigest() == old_record_hash.hexdigest():
            raise RedundantUpdateError('Redundant update, old and new record are identical')

    def _check_optimistic_lock(self, current_revision, version):
        if current_revision.version != version:
            raise OptimisticLockError('Record %s version on the DB is %d, your record\'s version is %d' %
                                      (current_revision.record_id, current_revision.version, version))

    def _get_current_revision(self, record_id):
        drf = self._get_drivers_factory(self.ehr_repository)
        with drf.get_driver() as driver:
            rec = driver.get_record_by_id(record_id)
            if not rec:
                raise OptimisticLockError('Record %s was deleted form the DB' % record_id)
            else:
                return driver.decode_record(rec)

    def _move_record_to_archive(self, record):
        # add version number to record's ID in order to avoid duplicated
        # IDs and to ensure that only one record with that version number
        # will exists in the archive
        record.record_id = {
            '_id': record.record_id,
            '_version': record.version
        }
        drf = self._get_drivers_factory(self.ehr_versioning_repository)
        with drf.get_driver() as driver:
            record_id = driver.add_record(driver.encode_record(record))
            return record_id

    def _get_record_from_archive(self, record_id, version_number):
        if not isinstance(version_number, int) or version_number < 1:
            raise ValueError('%r is not a valid version number' % version_number)
        drf = self._get_drivers_factory(self.ehr_versioning_repository)
        with drf.get_driver() as driver:
            record_id = {'_id': record_id, '_version': version_number}
            record = driver.get_record_by_id(record_id)
            if record:
                # use the "standard" form for the record's ID
                record['_id'] = record['_id']['_id']
                return driver.decode_record(record)
            else:
                return None

    def update_record(self, new_record):
        current_revision = self._get_current_revision(new_record.record_id)
        self._check_redundant_update(new_record, current_revision)
        self._check_optimistic_lock(current_revision, new_record.version)
        self._move_record_to_archive(current_revision)
        drf = self._get_drivers_factory(self.ehr_repository)
        with drf.get_driver() as driver:
            new_record.version += 1
            last_update = driver.replace_record(new_record.record_id,
                                                driver.encode_record(new_record),
                                                'last_update')
            new_record.last_update = last_update
        return new_record

    def update_field(self, record, field, value, last_update_label=None):
        current_revision = self._get_current_revision(record.record_id)
        if getattr(current_revision, field) == value:
            raise RedundantUpdateError('Field %s value for record %r is already %r' % (field,
                                                                                       record.record_id,
                                                                                       value))
        self._check_optimistic_lock(current_revision, record.version)
        self._move_record_to_archive(current_revision)
        drf = self._get_drivers_factory(self.ehr_repository)
        with drf.get_driver() as driver:
            last_update = driver.update_field(record.record_id, field, value, last_update_label, True)
        record.last_update = last_update
        record.version += 1
        setattr(record, field, value)
        return record

    def add_to_list(self, record, list_label, element, last_update_label):
        current_revision = self._get_current_revision(record.record_id)
        self._check_optimistic_lock(current_revision, record.version)
        self._move_record_to_archive(current_revision)
        drf = self._get_drivers_factory(self.ehr_repository)
        with drf.get_driver() as driver:
            last_update = driver.add_to_list(record.record_id, list_label, element,
                                             last_update_label, True)
        record.last_update = last_update
        record.version += 1
        return record

    def extend_list(self, record, list_label, elements, last_update_label):
        current_revision = self._get_current_revision(record.record_id)
        self._check_optimistic_lock(current_revision, record.version)
        self._move_record_to_archive(current_revision)
        drf = self._get_drivers_factory(self.ehr_repository)
        with drf.get_driver() as driver:
            last_update = driver.extend_list(record.record_id, list_label, elements,
                                             last_update_label, True)
        record.last_update = last_update
        record.version += 1
        return record

    def remove_from_list(self, record, list_label, element, last_update_label):
        current_revision = self._get_current_revision(record.record_id)
        self._check_optimistic_lock(current_revision, record.version)
        self._move_record_to_archive(current_revision)
        drf = self._get_drivers_factory(self.ehr_repository)
        with drf.get_driver() as driver:
            last_update = driver.remove_from_list(record.record_id, list_label, element,
                                                  last_update_label, True)
        record.last_update = last_update
        record.version += 1
        return record

    def restore_version(self, record_id, version):
        original_record = self._get_record_from_archive(record_id, version)
        if not original_record:
            raise RecordRestoreFailedError('Unable to retrieve version %d for record %s' %
                                           (version, record_id))
        drf = self._get_drivers_factory(self.ehr_repository)
        with drf.get_driver() as driver:
            driver.delete_record(record_id)
            driver.add_record(driver.encode_record(original_record))
        drf = self._get_drivers_factory(self.ehr_versioning_repository)
        with drf.get_driver() as driver:
            del_count = driver.delete_later_versions(record_id, version-1)
        return original_record, del_count

    def restore_original(self, record_id):
        return self.restore_version(record_id, version=1)

    def remove_revisions(self, record_id):
        drf = self._get_drivers_factory(self.ehr_versioning_repository)
        with drf.get_driver() as driver:
            del_count = driver.delete_later_versions(record_id)
        self.logger.debug('Removed %d revisions for record %s', del_count, record_id)
        return del_count