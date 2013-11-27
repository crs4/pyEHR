from abc import ABCMeta, abstractmethod
from openehr.utils import decode_dict
import time


class Record(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def __init__(self, creation_time):
        self.creation_time = creation_time
        self.last_update = creation_time
        self.active = True

    @abstractmethod
    def _to_document(self):
        doc = {
            'creation_time': self.creation_time,
            'last_update': self.last_update,
            'active': self.active
        }
        return doc

    @abstractmethod
    def _from_document(self, doc):
        doc = decode_dict(doc)
        self.creation_time = doc['creation_time']
        self.last_update = doc['last_update']
        self.active = doc['active']


class PatientRecord(Record):

    def __init__(self, ehr_records=None):
        super(PatientRecord, self).__init__(time.time())
        self.ehr_records = ehr_records or []

    def _to_document(self):
        doc = super(PatientRecord, self)._to_document()
        doc['ehr_records'] = self.ehr_records
        return doc

    def _from_document(self, doc):
        doc = decode_dict(doc)
        super(PatientRecord, self)._from_document(doc)
        self.ehr_records = doc['ehr_records']


class ClinicalRecord(Record):

    def __init__(self, ehr_data):
        super(ClinicalRecord, self).__init__(time.time())
        self.ehr_data = ehr_data

    def _to_document(self):
        doc = super(ClinicalRecord, self)._to_document()
        doc['ehr_data'] = self.ehr_data
        return doc

    def _from_document(self, doc):
        doc = decode_dict(doc)
        super(ClinicalRecord, self)._from_document(doc)
        doc['ehr_data'] = doc['ehr_data']
