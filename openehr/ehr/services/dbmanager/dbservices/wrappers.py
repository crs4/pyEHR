from abc import ABCMeta, abstractmethod
from voluptuous import Schema, Required, MultipleInvalid, Coerce
import time
from bson import ObjectId

from openehr.ehr.services.dbmanager.errors import InvalidJsonStructureError
from openehr.utils import cleanup_json, decode_dict


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

    @abstractmethod
    def to_json(self):
        pass

    @staticmethod
    @abstractmethod
    def from_json(json_data):
        pass


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

    def get_clinical_record_by_id(self, clinical_record_id):
        """
        Get a :class:`ClinicalRecord` related to the current :class:`PatientRecord`
        by specifying its ID

        :param clinical_record_id: the ID of the :class:`ClinicalRecord` that is going
        to be retrieved
        :type clinical_record_id: the ID as a String or as an ObjectId
        :return: the :class:`ClinicalRecord` if the ID was matched or None
        :rtype: :class:`ClinicalRecord` or None

        """
        for e in self.ehr_records:
            if str(e.record_id) == str(clinical_record_id):
                return e
        return None

    def to_json(self):
        """
        Encode current record into a JSON dictionary

        :return: a JSON dictionary
        :rtype: dictionary
        """
        attrs = ['record_id', 'creation_time', 'last_update', 'active']
        json = dict()
        for a in attrs:
            json[a] = getattr(self, a)
        json['ehr_records'] = []
        for e in self.ehr_records:
            json['ehr_records'].append(e.to_json())
        return json

    @staticmethod
    def from_json(json_data):
        """
        Create a :class:`PatientRecord` object from the given JSON dictionary, if one or more :class:`ClinicalRecord`
        objects in JSON format are encoded in ehr_records field, create these objects as well

        :param json_data: the JSON corresponding to the :class:`PatientRecord` object
        :type json_data: dictionary
        :return: a :class:`PatientRecord` object
        :rtype: :class:`PatientRecord`
        """
        schema = Schema({
            'creation_time': float,
            'last_update': float,
            'record_id': str,
            'active': bool,
            Required('ehr_records'): list
        })
        try:
            json_data = cleanup_json(decode_dict(json_data))
            schema(json_data)
            ehr_records = [ClinicalRecord.from_json(ehr) for ehr in json_data['ehr_records']]
            json_data['ehr_records'] = ehr_records
            return PatientRecord(**json_data)
        except MultipleInvalid:
            raise InvalidJsonStructureError('JSON record\'s structure is not compatible with PatientRecord object')


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

    def to_json(self):
        """
        Encode current record into a JSON dictionary

        :return: a JSON dictionary
        :rtype: dictionary
        """
        attrs = ['creation_time', 'last_update', 'active',
                 'archetype', 'ehr_data']
        json = dict()
        for a in attrs:
            json[a] = getattr(self, a)
        if self.record_id:
            json['record_id'] = str(self.record_id)
        return json

    @staticmethod
    def from_json(json_data):
        """
        Create a :class:`ClinicalRecord` object from the given JSON dictionary

        :param json_data: the JSON corresponding to the :class:`ClinicalRecord` object
        :type json_data: dictionary
        :return: a :class:`ClinicalRecord` object
        :rtype: :class:`ClinicalRecord`
        """
        schema = Schema({
            Required('archetype'): str,
            Required('ehr_data'): dict,
            'creation_time': float,
            'last_update': float,
            'active': bool,
            'record_id': Coerce(ObjectId),
        })
        try:
            json_data = cleanup_json(decode_dict(json_data))
            schema(json_data)
            if 'record_id' in json_data:
                json_data['record_id'] = ObjectId(json_data['record_id'])
            return ClinicalRecord(**json_data)
        except MultipleInvalid:
            raise InvalidJsonStructureError('JSON record\'s structure is not compatible with ClinicalRecord object')