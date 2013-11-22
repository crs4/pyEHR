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