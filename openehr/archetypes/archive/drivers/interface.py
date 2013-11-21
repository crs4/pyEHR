class DriverInterface(object):

    def save_archetype(self, archetype):
        raise NotImplementedError()

    def get_archetype_by_id(self, archetype_id, fetch_object_models=False):
        raise NotImplementedError()

    def get_archetypes(self, reference_model=None,
                       version=None, fetch_object_models=False):
        raise NotImplementedError()

    def delete_archetype(self, archetype):
        raise NotImplementedError()

    def add_object_model(self, archetype, object_model):
        raise NotImplementedError()

    def add_object_models(self, archetype, object_models):
        om_ids = []
        for o in object_models:
            om_ids.append(self.add_object_model(archetype, o))
        return om_ids

    def load_object_models(self, archetype):
        raise NotImplementedError()

    def load_object_model(self, archetype, object_model_format):
        raise NotImplementedError()

    def delete_object_model(self, update_archetype=True):
        raise NotImplementedError()