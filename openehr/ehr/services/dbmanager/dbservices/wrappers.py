from abc import ABCMeta, abstractmethod
from openehr.utils import decode_dict
import time


class RecordsFactory(object):

    @staticmethod
    def create_patient_record(document):
        """
        >>> doc = {
        ...   u'creation_time': 1385569688.39202, 'last_update': 1385570688.39202,
        ...   u'active': True, u'ehr_records': []
        ... }
        >>> record = RecordsFactory.create_patient_record(doc)
        >>> print('%f %f %r %r' % (record.creation_time, record.last_update,
        ...       record.active, record.ehr_records))
        1385569688.392020 1385570688.392020 True []
        """
        document = decode_dict(document)
        return PatientRecord(document['ehr_records'],
                             document['creation_time'],
                             document['last_update'],
                             document['active'])

    @staticmethod
    def create_clinical_record(document):
        """
        >>> doc = {
        ...   u'creation_time': 1385569688.39202, u'last_update': 1385570688.39202,
        ...   u'active': True, u'ehr_data': {'field1': 'value1', 'field2': 'value2'}
        ... }
        >>> record = RecordsFactory.create_clinical_record(doc)
        >>> print ('%f %f %r %r' % (record.creation_time, record.last_update,
        ...        record.active, record.ehr_data))
        1385569688.392020 1385570688.392020 True {'field2': 'value2', 'field1': 'value1'}
        """
        document = decode_dict(document)
        return ClinicalRecord(document['ehr_data'],
                              document['creation_time'],
                              document['last_update'],
                              document['active'])


class Record(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def __init__(self, creation_time, last_update=None, active=True):
        self.creation_time = creation_time
        self.last_update = last_update or creation_time
        self.active = active

    @abstractmethod
    def _to_document(self):
        doc = {
            'creation_time': self.creation_time,
            'last_update': self.last_update,
            'active': self.active
        }
        return doc


class PatientRecord(Record):

    def __init__(self, ehr_records=None, creation_time=None,
                 last_update=None, active=True):
        super(PatientRecord, self).__init__(creation_time or time.time(),
                                            last_update, active)
        self.ehr_records = ehr_records or []

    def _to_document(self):
        doc = super(PatientRecord, self)._to_document()
        doc['ehr_records'] = self.ehr_records
        return doc


class ClinicalRecord(Record):

    def __init__(self, ehr_data, creation_time=None, last_update=None,
                 active=True):
        super(ClinicalRecord, self).__init__(creation_time or time.time(),
                                             last_update, active)
        self.ehr_data = ehr_data

    def _to_document(self):
        doc = super(ClinicalRecord, self)._to_document()
        doc['ehr_data'] = self.ehr_data
        return doc
