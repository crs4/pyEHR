import sys, os, argparse, json

from bottle import route, run, Response, request

from openehr.utils import get_logger
from openehr.utils.services import get_service_configuration
from openehr.ehr.services.dbmanager.dbservices import DBServices


class DBService(object):

    def __init__(self, driver, host, database, patients_repository=None,
                 ehr_repository=None, port=None, user=None, passwd=None):
        self.logger = get_logger('db_service_daemon')
        self.dbs = DBServices(driver, host, database, patients_repository,
                              ehr_repository, port, user, passwd, self.logger)
        #######################################################
        # Web Service methods
        #######################################################
        route('/test')(self.test_service)

    def test_service(self):
        resp = Response(body=json.dumps({'key1': 'value1'}))
        return resp

    def start_service(self, host, port):
        self.logger.info('Starting DBService daemon')
        try:
            run(host=host, port=port, debug=True)
        except Exception, e:
            self.logger.critical('An errore has occurred: %s', e)


def get_parser():
    parser = argparse.ArgumentParser('Run the DBService daemon')
    parser.add_argument('--config', type=str, required=True,
                        help='service configuration file')
    return parser


def main(argv):
    parser = get_parser()
    args = parser.parse_args(argv)
    conf = get_service_configuration(args.config, get_logger('db_service_daemon_main'))
    if not conf:
        msg = 'It was impossible to load configuration, exit'
        sys.exit(msg)
    dbs = DBService(**conf.get_db_configuration())
    dbs.start_service(**conf.get_service_configuration())


if __name__ == '__main__':
    main(sys.argv[1:])