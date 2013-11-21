__author__ = 'ciccio'

from openehr.ehr.services.dbmanager.errors import *

class DriverInterface(object):

    def connect(self):
        raise NotImplementedError()

    def close(self):
        raise NotImplementedError()

    def executeQuery(self, query):
        raise NotImplementedError()