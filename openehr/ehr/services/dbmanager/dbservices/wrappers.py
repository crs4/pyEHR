from abc import ABCMeta, abstractmethod
import time


class Record(object):
    """
    Generic record abstract class containing record's base fields.

    :ivar record_id: record's unique identifier
    :ivar creation_time: timestamp of record's creation
    :ivar last_update: timestamp of the last update occurred on the record
    :ivar active: boolean representing if the record is active or not
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def __init__(self, creation_time, last_update=None, active=True, record_id=None):
        self.creation_time = creation_time
        self.last_update = last_update or creation_time
        self.active = active
        self.record_id = record_id


class PatientRecord(Record):
    """
    Class representing a patient's record

    :ivar ehr_records: the list of clinical records related to this patient
    """

    def __init__(self, ehr_records=None, creation_time=None,
                 last_update=None, active=True, record_id=None):
        super(PatientRecord, self).__init__(creation_time or time.time(),
                                            last_update, active, record_id)
        self.ehr_records = ehr_records or []


class ClinicalRecord(Record):
    """
    Class representing a clinical record

    :ivar archetype: the OpenEHR archetype class related to this clinical record
    :ivar ehr_data: clinical data in OpenEHR syntax
    """

    def __init__(self, archetype, ehr_data, creation_time=None, last_update=None,
                 active=True, record_id=None):
        super(ClinicalRecord, self).__init__(creation_time or time.time(),
                                             last_update, active, record_id)
        self.archetype = archetype
        self.ehr_data = ehr_data

