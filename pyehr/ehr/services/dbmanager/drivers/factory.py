from pyehr.utils import get_logger
from pyehr.ehr.services.dbmanager.errors import UnknownDriverError


class DriversFactory(object):

    def __init__(self, driver, host, database, repository=None,
                 port=None, user=None, passwd=None, index_service=None,
                 logger=None):
        self.driver = driver
        self.host = host
        self.database = database
        self.repository = repository
        self.port = port
        self.user = user
        self.passwd = passwd
        self.index_service = index_service
        self.logger = logger or get_logger('drivers-factory')

    def get_driver(self):
        if self.driver == 'mongodb':
            from pymongo import version as vsn
            if int(vsn.split(".")[0]) < 3:
                from mongo_pm2 import MongoDriverPM2
                return MongoDriverPM2(self.host, self.database, self.repository,
                               self.port, self.user, self.passwd,
                               self.index_service, self.logger)
            else:
                from mongo_pm3 import MongoDriverPM3
                return MongoDriverPM3(self.host, self.database, self.repository,
                               self.port, self.user, self.passwd,
                               self.index_service, self.logger)
        elif self.driver == 'elasticsearch':
            from elastic_search import ElasticSearchDriver
            return ElasticSearchDriver([{"host":self.host,"port":self.port}],
                                       self.database, self.repository,
                                       user=self.user, passwd=self.passwd,
                                       index_service=self.index_service, logger=self.logger)
        else:
            raise UnknownDriverError('Unknown driver: %s' % self.driver)