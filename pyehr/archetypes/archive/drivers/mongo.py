import pymongo
import pymongo.errors as pym_err
from interface import DriverInterface
from pyehr.utils import get_logger, decode_dict, decode_list
import pyehr.archetypes.archive.errors as oehr_errors
from pyehr.archetypes import ArchetypeWrapper, ObjectModelWrapper


class MongoDBManager(DriverInterface):

    def __init__(self, host, database, collection,
                 port=None, user=None, passwd=None,
                 logger=None):
        self.client = pymongo.MongoClient(host, port)
        self.database = self.client[database]
        if user:
            self.database.authenticate(user, passwd)
        self.collection = self.database[collection]
        self.logger = logger or get_logger('mongo-db-driver')

    def __del__(self):
        self.client.disconnect()

    def __archetype_to_document(self, archetype, remove_object_model=True):
        arch = {
            '_id': archetype.archetype_id,
            'document_type': 'ARCHETYPE',
            'label': archetype.label,
            'reference_model': archetype.reference_model,
            'version': archetype.version,
            'object_models': []
        }
        if not remove_object_model:
            for obj in archetype.object_models:
                arch['object_models'].append(self.__object_model_to_document(obj))
        return arch

    def __document_to_archetype(self, document):
        # TODO: check if validation is needed
        document = decode_dict(document)
        obj_models = [self.__document_to_object_model(o) for o in document['object_models']]
        arch = ArchetypeWrapper(document['_id'], document['label'],
                                document['reference_model'], document['version'],
                                obj_models)
        return arch

    def __document_to_object_model(self, document):
        # TODO: check if validation is needed
        document = decode_dict(document)
        return ObjectModelWrapper(document['data'], document['format'],
                                  document['_id'])

    def __object_model_to_document(self, object_model):
        return {
            '_id': object_model.md5,
            'document_type': 'OBJECT_MODEL',
            'format': object_model.format,
            'data': object_model.data,
        }

    def __save_document(self, doc):
        doc_id = self.collection.insert(doc)
        return doc_id

    def __delete_document(self, document_id):
        self.collection.remove(document_id)

    def __update_document(self, document_id, update_statement):
        self.collection.update({'_id': document_id}, update_statement)

    def __get_document_by_id(self, document_id):
        return self.collection.find_one(document_id)

    def __available_object_model_formats(self, archetype_id):
        objm_ids = self.collection.find_one(archetype_id,
                                            {'object_models': True, '_id': False})['object_models']
        if len(objm_ids) == 0:
            return list()
        else:
            formats = [x['format'] for x in
                       self.collection.find({'_id': {'$in': objm_ids}},
                                            {'format': True, '_id': False})]
            return decode_list(formats)

    def save_archetype(self, archetype):
        arch = self.__archetype_to_document(archetype)
        arch['object_models'] = []
        try:
            arch_id = self.__save_document(arch)
        except pym_err.DuplicateKeyError, dke:
            msg = 'Archetype with ID %s already exists' % arch['_id']
            self.logger.error(msg)
            raise oehr_errors.DuplicatedArchetypeError(msg)
        # reload document when appending object models
        objm_ids = self.add_object_models(self.get_archetype_by_id(arch_id),
                                          archetype.object_models)
        return arch_id, objm_ids

    def __fetch_object_models(self, object_model_ids):
        return [self.__get_document_by_id(o_id) for o_id in object_model_ids]

    def get_archetypes(self, reference_model=None,
                       version=None, fetch_object_models=False):
        selector = {'document_type': 'ARCHETYPE'}
        if reference_model:
            selector['reference_model'] = reference_model
        if version:
            selector['version'] = version
        archetypes = []
        for doc in self.collection.find(selector):
            if fetch_object_models:
                doc['object_models'] = self.__fetch_object_models(doc['object_models'])
            else:
                doc['object_models'] = []
            archetypes.append(self.__document_to_archetype(doc))
        return archetypes

    def get_archetype_by_id(self, archetype_id, fetch_object_models=False):
        arch = self.__get_document_by_id(archetype_id)
        if arch:
            if fetch_object_models:
                arch['object_models'] = self.__fetch_object_models(arch['object_models'])
            else:
                arch['object_models'] = []
            return self.__document_to_archetype(arch)
        else:
            return None

    def delete_archetype(self, archetype):
        if len(archetype.object_models) == 0:
            # try to fetch object models
            archetype = self.load_object_models(archetype)
        for objm in archetype.object_models:
            self.delete_object_model(objm, False)
        self.__delete_document(archetype.archetype_id)

    def __get_connected_archetype(self, object_model):
        a = self.collection.find_one({'object_models': object_model.md5})
        return a

    def delete_object_model(self, object_model, update_archetype=True):
        self.__delete_document(object_model.md5)
        if update_archetype:
            archetype = self.__get_connected_archetype(object_model)
            self.__update_document(archetype['_id'], {'$pull': {'object_models': object_model.md5}})

    def add_object_model(self, archetype, object_model):
        objm = self.__object_model_to_document(object_model)
        try:
            objm_id = self.__save_document(objm)
        except pym_err.DuplicateKeyError, dke:
            msg = 'Object Model with ID %s already exists' % objm['_id']
            self.logger.error(msg)
            raise oehr_errors.DuplicatedObjectModelError(msg)
        # the $addToSet statement appends the element to the list only if the
        # element itself does not belong to the list already
        self.__update_document(archetype.archetype_id,
                               {'$addToSet': {'object_models': objm_id}})
        return objm_id

    def add_object_models(self, archetype, object_models):
        return super(MongoDBManager, self).add_object_models

    def load_object_models(self, archetype):
        return self.get_archetype_by_id(archetype.archetype_id,
                                        fetch_object_models=True)

    def load_object_model(self, archetype, object_model_format):
        arc = self.__get_document_by_id(archetype.archetype_id)
        object_model = self.collection.find_one({
            '_id': {
                '$in': arc['object_models']
            },
            'format': object_model_format
        })
        if not object_model:
            msg = 'No object model with format %s connected to archetype %s' % (
                object_model_format,
                archetype.archetype_id
            )
            raise oehr_errors.MissingObjectModelFormatError(msg)
        archetype.object_models = [self.__document_to_object_model(object_model)]
        return archetype
