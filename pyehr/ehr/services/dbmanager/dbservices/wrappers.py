from abc import ABCMeta, abstractmethod
from voluptuous import Schema, Required, MultipleInvalid, Coerce
import time
from uuid import uuid4

from pyehr.ehr.services.dbmanager.errors import InvalidJsonStructureError,\
    OperationNotAllowedError
from pyehr.utils import cleanup_json, decode_dict


class Record(object):
    """
    Generic record abstract class containing record's base fields.

    :ivar record_id: record's unique identifier
    :ivar creation_time: timestamp of record's creation
    :ivar last_update: timestamp of the last update occurred on the record
    :ivar active: boolean representing if the record is active or not
    """

    __metaclass__ = ABCMeta

    def __eq__(self, other):
        if type(self) == type(other):
            return (self.record_id == other.record_id) and \
                   (not self.record_id is None and not other.record_id is None)
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    @abstractmethod
    def __init__(self, creation_time, last_update=None, active=True, record_id=None):
        self.creation_time = creation_time
        self.last_update = last_update or creation_time
        self.active = active
        if record_id:
            self.record_id = str(record_id)
        else:
            self.record_id = uuid4().hex
            
    @abstractmethod
    def new_record_id(self):
        self.record_id = uuid4().hex

    @abstractmethod
    def to_json(self):
        pass

    @classmethod
    @abstractmethod
    def from_json(cls, json_data):
        pass


class PatientRecord(Record):
    """
    Class representing a patient's record

    :ivar ehr_records: the list of clinical records related to this patient
    """

    def __init__(self, record_id, ehr_records=None, creation_time=None,
                 last_update=None, active=True):
        super(PatientRecord, self).__init__(creation_time or time.time(),
                                            last_update, active, record_id)
        self.ehr_records = ehr_records or []
        
    def new_record_id(self):
        pass

    def get_clinical_record_by_id(self, clinical_record_id):
        """
        Get a :class:`ClinicalRecord` related to the current :class:`PatientRecord`
        by specifying its ID

        :param clinical_record_id: the ID of the :class:`ClinicalRecord` that is going
        to be retrieved
        :type clinical_record_id: the ID as a String
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

    @classmethod
    def from_json(cls, json_data):
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
            json_data = decode_dict(json_data)
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

    def __init__(self, ehr_data, creation_time=None, last_update=None,
                 active=True, record_id=None, structure_id=None,
                 version=0):
        super(ClinicalRecord, self).__init__(creation_time or time.time(),
                                             last_update, active, record_id)
        self.ehr_data = ehr_data
        self.patient_id = None
        self.structure_id = structure_id
        self._version = version

    @property
    def version(self):
        return self._version

    @version.setter
    def version(self, _version):
        if not isinstance(_version, int):
            raise ValueError('Version must be an integer, %s was given' % type(_version))
        if _version >= 0:
            self._version = _version
        else:
            raise ValueError('Wrong value for version field: %r' % _version)

    @property
    def is_persistent(self):
        return self.version != 0

    def reset(self):
        self.new_record_id()
        self.unbind_from_patient()
        self.reset_version()
        
    def new_record_id(self):
        super(ClinicalRecord, self).new_record_id()
        self.reset_version()

    def unbind_from_patient(self):
        self.patient_id = None

    def reset_version(self):
        self.version = 0

    def increase_version(self):
        self.version += 1

    def _set_patient_id(self, patient_id):
        self.patient_id = patient_id

    def bind_to_patient(self, patient):
        """
        Bind the current :class:`ClinicalRecord` instance to a given :class:`PatientRecord`
        :param patient:
        :return:
        """
        self._set_patient_id(patient.record_id)

    def to_json(self):
        """
        Encode current record into a JSON dictionary

        :return: a JSON dictionary
        :rtype: dictionary
        """
        attrs = ['creation_time', 'last_update', 'active', 'version']
        json = dict()
        for a in attrs:
            json[a] = getattr(self, a)
        if self.record_id:
            json['record_id'] = self.record_id
        if self.patient_id:
            json['patient_id'] = str(self.patient_id)
        json['ehr_data'] = self.ehr_data.to_json()
        return json

    @staticmethod
    def _get_validation_schema():
        return Schema({
            Required('ehr_data'): dict,
            'creation_time': float,
            'last_update': float,
            'active': bool,
            'record_id': str,
            'patient_id': str,
            'version': int
        })

    @classmethod
    def from_json(cls, json_data):
        """
        Create a :class:`ClinicalRecord` object from the given JSON dictionary

        :param json_data: the JSON corresponding to the :class:`ClinicalRecord` object
        :type json_data: dictionary
        :return: a :class:`ClinicalRecord` object
        :rtype: :class:`ClinicalRecord`
        """
        schema = cls._get_validation_schema()
        try:
            json_data = decode_dict(json_data)
            schema(json_data)
            json_data['ehr_data'] = ArchetypeInstance.from_json(json_data['ehr_data'])
            try:
                patient_id = json_data.pop('patient_id')
            except KeyError:
                patient_id = None
            crec = ClinicalRecord(**json_data)
            if patient_id:
                crec._set_patient_id(patient_id)
            return crec
        except MultipleInvalid:
            raise InvalidJsonStructureError('JSON record\'s structure is not compatible with ClinicalRecord object')

    def convert_to_revision(self):
        if not self.is_persistent:
            raise OperationNotAllowedError('Non persistent record can\'t be converted to ClinicalRecordRevision')
        return ClinicalRecordRevision(
            ehr_data=self.ehr_data,
            record_id={'_id': self.record_id, '_version': self.version},
            patient_id=self.patient_id,
            creation_time=self.creation_time,
            last_update=self.last_update,
            active=self.active,
            structure_id=self.structure_id,
            version=self.version
        )


class ClinicalRecordRevision(ClinicalRecord):

    def __init__(self, ehr_data, record_id, patient_id, creation_time=None, last_update=None,
                 active=True, structure_id=None, version=0):
        super(ClinicalRecordRevision, self).__init__(ehr_data, creation_time, last_update,
                                                     active, record_id, structure_id, version)
        self.record_id = record_id
        self._set_patient_id(patient_id)

    @staticmethod
    def _get_validation_schema():
        id_schema = Schema({'_id': str, '_version': int}, required=True)
        return Schema({
            Required('ehr_data'): dict,
            Required('record_id'): id_schema,
            'creation_time': float,
            'last_update': float,
            'active': bool,
            'record_id': str,
            'patient_id': str,
            'version': int
        })

    def convert_to_clinical_record(self):
        crec = ClinicalRecord(
            ehr_data=self.ehr_data,
            creation_time=self.creation_time,
            last_update=self.last_update,
            active=self.active,
            record_id=self.record_id['_id'],
            structure_id=self.structure_id,
            version=self.version
        )
        crec._set_patient_id(self.patient_id)
        return crec


class ArchetypeInstance(object):
    """
    Class representing an openEHR Archetype instance

    :ivar archetype_class: the openEHR Archetype class related to this instance
    :ivar archetype_details: clinical data related to this instance represented as a dictionary.
                             Values of the dictionary can be :class:`ArchetypeInstance` objects.
    """

    def __init__(self, archetype_class, archetype_details):
        self.archetype_class = archetype_class
        self.archetype_details = archetype_details

    def to_json(self):
        """
        Encode current record into a JSON dictionary

        :return: a JSON dictionary
        :rtype: dictionary
        """
        def encode_dict_data(record_data):
            data = dict()
            for k, v in record_data.iteritems():
                if isinstance(v, ArchetypeInstance):
                    data[k] = v.to_json()
                elif isinstance(v, dict):
                    data[k] = encode_dict_data(v)
                elif isinstance(v, list):
                    data[k] = encode_list_data(v)
                else:
                    data[k] = v
            return data

        def encode_list_data(record_data):
            data = list()
            for x in record_data:
                if isinstance(x, ArchetypeInstance):
                    data.append(x.to_json())
                elif isinstance(x, dict):
                    data.append(encode_dict_data(x))
                elif isinstance(x, list):
                    data.append(encode_list_data(x))
                else:
                    data.append(x)
            return data

        json = {
            'archetype_class': self.archetype_class,
            'archetype_details': dict()
        }
        for k, v in self.archetype_details.iteritems():
            if isinstance(v, ArchetypeInstance):
                json['archetype_details'][k] = v.to_json()
            elif isinstance(v, dict):
                json['archetype_details'][k] = encode_dict_data(v)
            elif isinstance(v, list):
                json['archetype_details'][k] = encode_list_data(v)
            else:
                json['archetype_details'][k] = v
        return json

    @staticmethod
    def from_json(json_data):
        """
        Create an :class:`ArchetypeInstance` object from a given JSON dictionary

        :param json_data: the JSON corresponding to the :class:`ArchetypeInstance` object
        :type json_data: dictionary
        :return: an :class:`ArchetypeInstance` object
        :rtype: :class:`ArchetypeInstance`
        """
        def is_archetype(dict):
            return ('archetype_class' in dict) and ('archetype_details' in dict)

        def decode_dict_data(dict_data):
            data = dict()
            for k, v in dict_data.iteritems():
                if isinstance(v, dict):
                    if is_archetype(v):
                        data[k] = ArchetypeInstance.from_json(v)
                    else:
                        data[k] = decode_dict_data(v)
                elif isinstance(v, list):
                    data[k] = decode_list_data(v)
                else:
                    data[k] = v
            return data

        def decode_list_data(dict_data):
            data = list()
            for x in dict_data:
                if isinstance(x, dict):
                    if is_archetype(x):
                        data.append(ArchetypeInstance.from_json(x))
                    else:
                        data.append(decode_dict_data(x))
                elif isinstance(x, list):
                    data.append(decode_list_data(x))
                else:
                    data.append(x)
            return data

        schema = Schema({
            Required('archetype_class'): str,
            Required('archetype_details'): dict,
        })
        try:
            json_data = cleanup_json(decode_dict(json_data))
            schema(json_data)
            archetype_data = dict()
            for k, v in json_data['archetype_details'].iteritems():
                if isinstance(v, dict):
                    if is_archetype(v):
                        archetype_data[k] = ArchetypeInstance.from_json(v)
                    else:
                        archetype_data[k] = decode_dict_data(v)
                elif isinstance(v, list):
                    archetype_data[k] = decode_list_data(v)
                else:
                    archetype_data[k] = v
            return ArchetypeInstance(json_data['archetype_class'], archetype_data)
        except MultipleInvalid:
            raise InvalidJsonStructureError('JSON record\'s structure is not compatible with ArchetypeInstance object')