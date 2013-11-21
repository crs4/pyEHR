from hashlib import md5


class ArchetypeWrapper(object):

    OBSERVATION = 'openEHR-EHR-OBSERVATION'
    ACTION = 'openEHR-EHR-ACTION'
    EVALUATION = 'openEHR-EHR-EVALUATION'
    INSTRUCTION = 'openEHR-EHR-INSTRUCTION'
    ADMIN_ENTRY = 'openEHR-EHR-ADMIN_ENTRY'

    def __init__(self, archetype_id, archetype_label=None, reference_model=None,
                 version=None, object_models=[]):
        label, ref_mod, vers = self.__metadata_from_id(archetype_id)
        self.archetype_id = archetype_id
        self.label = archetype_label or label
        self.reference_model = reference_model or ref_mod
        if self.reference_model not in (self.OBSERVATION, self.ACTION, self.EVALUATION,
                                        self.INSTRUCTION, self.ADMIN_ENTRY):
            raise ValueError('Bad reference model')
        self.version = version or vers
        if not self.version.startswith('v'):
            self.version = 'v%s' % self.version
        self.object_models = object_models

    def __metadata_from_id(self, archetype_id):
        rm, l, v = archetype_id.split('.')
        return l, rm, v

    def validate(self):
        raise NotImplementedError()


class ObjectModelWrapper(object):

    def __init__(self, data, format, md5_hash=None):
        self.format = format
        self.data = data
        if not md5_hash:
            self.md5 = md5(self.data).hexdigest()
        else:
            if md5_hash != md5(self.data).hexdigest():
                raise ValueError('given MD5 hash does not match with data MD5')
            self.md5 = md5_hash
