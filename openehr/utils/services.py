from ConfigParser import SafeConfigParser, NoOptionError


class ServiceConfig(object):

    def __init__(self, db_driver, db_host, db_database,
                 db_port, db_user, db_passwd,
                 db_patients_repository, db_ehr_repository,
                 service_host, service_port):
        self.db_driver = db_driver
        self.db_host = db_host
        self.db_database = db_database
        self.db_port = db_port,
        self.db_user = db_user
        self.db_passwd = db_passwd
        self.db_patients_repository = db_patients_repository
        self.db_ehr_repository = db_ehr_repository
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

    def get_service_configuration(self):
        return {
            'host': self.service_host,
            'port': self.service_port
        }


def get_service_configuration(configuration_file, logger):
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
            parser.get('service', 'host'),
            parser.get('service', 'port')
        )
        return conf
    except NoOptionError, nopt:
        logger.critical('Missing option %s in configuration file', nopt)
        return None
