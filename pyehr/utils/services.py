from ConfigParser import SafeConfigParser, NoOptionError
from pyehr.utils import get_logger
import logging
from logging.handlers import RotatingFileHandler


class ServiceConfig(object):

    def __init__(self, db_driver, db_host, db_database,
                 db_port, db_user, db_passwd,
                 db_patients_repository, db_ehr_repository,
                 index_host, index_port, index_database,
                 index_user, index_passwd,
                 service_host, service_port):
        self.db_driver = db_driver
        self.db_host = db_host
        self.db_database = db_database
        self.db_port = int(db_port)
        self.db_user = db_user
        self.db_passwd = db_passwd
        self.db_patients_repository = db_patients_repository
        self.db_ehr_repository = db_ehr_repository
        self.index_host = index_host
        self.index_port = index_port
        self.index_database = index_database
        self.index_user = index_user
        self.index_passwd = index_passwd
        self.service_host = service_host
        self.service_port = service_port

    def get_db_configuration(self):
        return {
            'driver': self.db_driver,
            'host': self.db_host,
            'database': self.db_database,
            'port': self.db_port,
            'user': self.db_user,
            'passwd': self.db_passwd,
            'patients_repository': self.db_patients_repository,
            'ehr_repository': self.db_ehr_repository
        }

    def get_index_configuration(self):
        return {
            'host': self.index_host,
            'port': self.index_port,
            'database': self.index_database,
            'user': self.index_user,
            'passwd': self.index_passwd
        }

    def get_service_configuration(self):
        return {
            'host': self.service_host,
            'port': self.service_port
        }


def get_service_configuration(configuration_file, logger=None):
    if not logger:
        logger = get_logger('service_configuration')
    parser = SafeConfigParser(allow_no_value=True)
    parser.read(configuration_file)
    try:
        conf = ServiceConfig(
            parser.get('db', 'driver'),
            parser.get('db', 'host'),
            parser.get('db', 'database'),
            parser.get('db', 'port'),
            parser.get('db', 'user'),
            parser.get('db', 'passwd'),
            parser.get('db', 'patients_repository'),
            parser.get('db', 'ehr_repository'),
            parser.get('index', 'host'),
            parser.get('index', 'port'),
            parser.get('index', 'database'),
            parser.get('index', 'user'),
            parser.get('index', 'passwd'),
            parser.get('service', 'host'),
            parser.get('service', 'port')
        )
        return conf
    except NoOptionError, nopt:
        logger.critical('Missing option %s in configuration file', nopt)
        return None


def get_rotating_file_logger(log_file, log_level, logger_label,
                             max_bytes=100*1024*1024, backup_count=3):
    log_format='%(asctime)s|%(levelname)-8s|%(message)s',
    log_datefmt='%Y-%m-%d %H:%M:%S'
    logger = logging.getLogger(logger_label)
    logger.setLevel(getattr(logging, log_level))
    handler = RotatingFileHandler(log_file, max_bytes, backup_count)
    handler.setLevel(getattr(logging, log_level))
    formatter = logging.Formatter(log_format, datefmt=log_datefmt)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger