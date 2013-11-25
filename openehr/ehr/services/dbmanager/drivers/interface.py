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