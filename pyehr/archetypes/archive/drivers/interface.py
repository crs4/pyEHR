from abc import ABCMeta, abstractmethod


class DriverInterface(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def save_archetype(self, archetype):
        pass

    @abstractmethod
    def get_archetype_by_id(self, archetype_id, fetch_object_models=False):
        pass

    @abstractmethod
    def get_archetypes(self, reference_model=None,
                       version=None, fetch_object_models=False):
        pass

    @abstractmethod
    def delete_archetype(self, archetype):
        pass

    @abstractmethod
    def add_object_model(self, archetype, object_model):
        pass

    @abstractmethod
    def add_object_models(self, archetype, object_models):
        om_ids = []
        for o in object_models:
            om_ids.append(self.add_object_model(archetype, o))
        return om_ids

    @abstractmethod
    def load_object_models(self, archetype):
        pass

    @abstractmethod
    def load_object_model(self, archetype, object_model_format):
        pass

    @abstractmethod
    def delete_object_model(self, update_archetype=True):
        pass