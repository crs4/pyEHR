from abc import ABCMeta, abstractmethod
from voluptuous import Schema, Required, MultipleInvalid, Coerce
import time
from bson import ObjectId

from openehr.ehr.services.dbmanager.errors import InvalidJsonStructureError
from openehr.utils import cleanup_json


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

    def to_json(self):
        """
        Encode current record into a JSON dictionary

        :return: a JSON dictionary
        :rtype: dictionary

        >>> crec = ClinicalRecord(ehr_data={'field1': 'value1', 'field2': 'value2'},
        ...                       archetype='openEHR-EHR-EVALUATION.dummy-evaluation.v1',
        ...                       creation_time=1393863996.14704,
        ...                       record_id = ObjectId('5314b3a55c98900a8a3d1a2c'))
        >>> prec = PatientRecord(ehr_records=[crec], creation_time=1393863996.14704,
        ...                      record_id='V01245AC14CE2412412340')
        >>> prec.to_json()
        {'record_id': 'V01245AC14CE2412412340', 'active': True, 'ehr_records': [{'ehr_data': {'field2': 'value2', \
'field1': 'value1'}, 'creation_time': 1393863996.14704, 'last_update': 1393863996.14704, 'record_id': \
'5314b3a55c98900a8a3d1a2c', 'active': True, 'archetype': 'openEHR-EHR-EVALUATION.dummy-evaluation.v1'}], \
'creation_time': 1393863996.14704, 'last_update': 1393863996.14704}
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

        >>> crec_json = {
        ...     'creation_time': 1393863996.14704,
        ...     'archetype': 'openEHR-EHR-EVALUATION.dummy-evaluation.v1',
        ...     'ehr_data': {'field1': 'value1', 'field2': 'value2'},
        ...     'record_id': '5314b3a55c98900a8a3d1a2c',
        ... }
        >>> prec_json = {
        ...     'creation_time': 1393863996.14704,
        ...     'record_id': 'V01245AC14CE2412412340',
        ...     'ehr_records': [crec_json]
        ... }
        >>> prec = PatientRecord.from_json(prec_json)
        >>> print type(prec)
        <class 'openehr.ehr.services.dbmanager.dbservices.wrappers.PatientRecord'>
        >>> for e in prec.ehr_records:
        ...     print type(e)
        ...     e.record_id
        <class 'openehr.ehr.services.dbmanager.dbservices.wrappers.ClinicalRecord'>
        ObjectId('5314b3a55c98900a8a3d1a2c')
        """
        schema = Schema({
            'creation_time': float,
            'last_update': float,
            'record_id': str,
            'active': bool,
            Required('ehr_records'): list
        })
        try:
            json_data = cleanup_json(json_data)
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
            json_data = cleanup_json(json_data)
            schema(json_data)
            if json_data.has_key('record_id'):
                json_data['record_id'] = ObjectId(json_data['record_id'])
            return ClinicalRecord(**json_data)
        except MultipleInvalid:
            raise InvalidJsonStructureError('JSON record\'s structure is not compatible with ClinicalRecord object')