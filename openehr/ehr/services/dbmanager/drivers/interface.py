from abc import ABCMeta, abstractmethod


class DriverInterface(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def disconnect(self):
        pass

    @abstractmethod
    def executeQuery(self, query):
        pass

    @abstractmethod
    def add_record(self, record):
        pass

    @abstractmethod
    def add_records(self, records):
        return [self.add_record(r) for r in records]

    @abstractmethod
    def get_record_by_id(self, record_id):
        pass

    @abstractmethod
    def get_all_records(self):
        pass

    @abstractmethod
    def delete_record(self, record_id):
        pass

    @abstractmethod
    def update_record(self, record_id, update_condition):
        pass

    @abstractmethod
    def update_field(self, record_id, field_label, field_value,
                     update_timestamp_label):
        pass

    @abstractmethod
    def add_to_list(self, record_id, list_label, item_value,
                    update_timestamp_label):
        pass

    @abstractmethod
    def remove_from_list(self, record_id, list_label, item_Value,
                         update_timestamp_label):
        pass
