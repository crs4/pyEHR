from ConfigParser import SafeConfigParser, NoOptionError
from pyehr.utils import get_logger, LOG_FORMAT, LOG_DATEFMT
import logging, os, sys
from logging.handlers import RotatingFileHandler


class ServiceConfig(object):

    def __init__(self, db_driver, db_host, db_database, db_versioning_database,
                 db_port, db_user, db_passwd, db_patients_repository,
                 db_ehr_repository, db_ehr_versioning_repository,
                 index_url, index_database, index_user, index_passwd,
                 db_service_host, db_service_port, db_service_server_engine,
                 query_service_host, query_service_port, query_service_server_engine):
        self.db_driver = db_driver
        self.db_host = db_host
        self.db_database = db_database
        self.db_versioning_database = db_versioning_database
        self.db_port = int(db_port)
        self.db_user = db_user
        self.db_passwd = db_passwd
        self.db_patients_repository = db_patients_repository
        self.db_ehr_repository = db_ehr_repository
        self.db_ehr_versioning_repository = db_ehr_versioning_repository
        self.index_url = index_url
        self.index_database = index_database
        self.index_user = index_user
        self.index_passwd = index_passwd
        self.db_service_host = db_service_host
        self.db_service_port = db_service_port
        self.db_service_server_engine = db_service_server_engine
        self.query_service_host = query_service_host
        self.query_service_port = query_service_port
        self.query_service_server_engine = query_service_server_engine

    def get_db_configuration(self):
        return {
            'driver': self.db_driver,
            'host': self.db_host,
            'database': self.db_database,
            'versioning_database': self.db_versioning_database,
            'port': self.db_port,
            'user': self.db_user,
            'passwd': self.db_passwd,
            'patients_repository': self.db_patients_repository,
            'ehr_repository': self.db_ehr_repository,
            'ehr_versioning_repository': self.db_ehr_versioning_repository
        }

    def get_index_configuration(self):
        return {
            'url': self.index_url,
            'database': self.index_database,
            'user': self.index_user,
            'passwd': self.index_passwd
        }

    def get_db_service_configuration(self):
        return {
            'host': self.db_service_host,
            'port': self.db_service_port,
            'engine': self.db_service_server_engine
        }

    def get_query_service_configuration(self):
        return {
            'host': self.query_service_host,
            'port': self.query_service_port,
            'engine': self.query_service_server_engine
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
            parser.get('db', 'versioning_database'),
            parser.get('db', 'port'),
            parser.get('db', 'user'),
            parser.get('db', 'passwd'),
            parser.get('db', 'patients_repository'),
            parser.get('db', 'ehr_repository'),
            parser.get('db', 'ehr_versioning_repository'),
            parser.get('index', 'url'),
            parser.get('index', 'database'),
            parser.get('index', 'user'),
            parser.get('index', 'passwd'),
            parser.get('db_service', 'host'),
            parser.get('db_service', 'port'),
            parser.get('db_service', 'server_engine'),
            parser.get('query_service', 'host'),
            parser.get('query_service', 'port'),
            parser.get('query_service', 'server_engine')
        )
        return conf
    except NoOptionError, nopt:
        logger.critical('Missing option %s in configuration file', nopt)
        return None


def get_rotating_file_logger(logger_label, log_file, log_level='INFO',
                             max_bytes=100*1024*1024, backup_count=3):
    logger = logging.getLogger(logger_label)
    if not isinstance(log_level, int):
        try:
            log_level = getattr(logging, log_level)
        except AttributeError:
            raise ValueError('unsupported literal log level: %s' % log_level)
    logger.setLevel(log_level)
    # clear existing handlers
    logger.handlers = []
    handler = RotatingFileHandler(log_file, max_bytes, backup_count)
    formatter = logging.Formatter(LOG_FORMAT, datefmt=LOG_DATEFMT)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


def check_pid_file(pid_file, logger):
    if os.path.isfile(pid_file):
        logger.info('Another dbservice daemon is running, exit')
        sys.exit(0)


def create_pid(pid_file):
    pid = str(os.getpid())
    with open(pid_file, 'w') as ofile:
        ofile.write(pid)


def destroy_pid(pid_file):
    os.remove(pid_file)