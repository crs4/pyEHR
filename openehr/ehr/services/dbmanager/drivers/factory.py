from openehr.utils import get_logger
from mongo import MongoDriver
from openehr.ehr.services.dbmanager.errors import UnknownDriverError


class DriversFactory(object):

    def __init__(self, driver, host, database, repository=None,
                 port=None, user=None, passwd=None, logger=None):
        self.driver = driver
        self.host = host
        self.database = database
        self.repository = repository
        self.port = port
        self.user = user
        self.passwd = passwd
        self.logger = logger or get_logger('drivers-factory')

    def get_driver(self):
        if self.driver == 'mongodb':
            return MongoDriver(self.host, self.database, self.repository,
                               self.port, self.user, self.passwd, self.logger)
        else:
            raise UnknownDriverError('Unknown driver: %s' % self.driver)