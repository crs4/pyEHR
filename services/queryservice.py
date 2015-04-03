import sys, argparse
from functools import wraps

try:
    import simplejson as json
except ImportError:
    import json

from bottle import post, get, run, response, request, abort, HTTPError

from pyehr.ehr.services.dbmanager.querymanager import QueryManager
from pyehr.utils import get_logger
from pyehr.utils.services import get_service_configuration, check_pid_file,\
    create_pid, destroy_pid, get_rotating_file_logger
import pyehr.ehr.services.dbmanager.errors as pyehr_errors


class QueryService():

    def __init__(self, driver, host, database, versioning_database,
                 patients_repository=None, ehr_repository=None,
                 ehr_versioning_repository=None,
                 port=None, user=None, passwd=None,
                 log_file=None, log_level='INFO'):
        if not log_file:
            self.logger = get_logger('query_service_daemon')
        else:
            self.logger = get_rotating_file_logger('query_service_daemon', log_file,
                                                   log_level=log_level)
        self.qmanager = QueryManager(driver, host, database, versioning_database,
                                     patients_repository, ehr_repository,
                                     ehr_versioning_repository,
                                     port, user, passwd, self.logger)
        ###############################################
        # Web Service methods
        ###############################################
        post('/query/execute')(self.execute_query)
        post('/query/execute_count')(self.execute_count_query)
        # utilities
        post('/check/status/querymanager')(self.test_server)
        get('/check/status/querymanager')(self.test_server)

    def add_index_service(self, url, database, user, passwd):
        self.qmanager.set_index_service(url, database, user, passwd)

    def exception_handler(f):
        @wraps(f)
        def wrapper(inst, *args, **kwargs):
            try:
                return f(inst, *args, **kwargs)
            except pyehr_errors.UnknownDriverError, ude:
                inst._error(str(ude), 500)
            except HTTPError:
                #if an abort was called in wrapped function, raise the generated HTTPError
                raise
            except Exception, e:
                import traceback
                inst.logger.error(traceback.print_stack())
                msg = 'Unexpected error: %s' % e.message
                inst._error(msg, 500)
        return wrapper

    def _error(self, msg, error_code):
        self.logger.error(msg)
        body = {
            'SUCCESS': False,
            'ERROR': msg
        }
        abort(error_code, json.dumps(body))

    def _missing_mandatory_field(self, field_label):
        msg = 'Missing mandatory field %s, can\'t continue with the request' % field_label
        self._error(msg, 400)

    def _success(self, body, return_code=200):
        response.content_type = 'application/json'
        response.status = return_code
        return body

    def _execute_query(self, params, count_only):
        aql_query = params.get('query')
        if not aql_query:
            self._missing_mandatory_field('query')
        query_params = params.get('query_params')
        if query_params:
            query_params = json.loads(query_params)
        results = self.qmanager.execute_aql_query(aql_query, query_params, count_only)
        return results

    @exception_handler
    def execute_query(self):
        params = request.forms
        results = self._execute_query(params, count_only=False)
        response_body = {
            'SUCCESS': True,
            'RESULTS_SET': results.to_json()
        }
        return self._success(response_body)

    @exception_handler
    def execute_count_query(self):
        params = request.forms
        results = self._execute_query(params, count_only=True)
        response_body = {
            'SUCCESS': True,
            'RESULTS_COUNTER': results
        }
        return self._success(response_body)

    def start_service(self, host, port, engine, debug=False):
        self.logger.info('Starting QueryService daemon with DEBUG set to %s', debug)
        try:
            run(host=host, port=port, server=engine, debug=debug)
        except Exception, e:
            self.logger.critical('An error has occurred: %s', e)

    def test_server(self):
        return 'QueryManager daemon running'


def get_parser():
    parser = argparse.ArgumentParser('Run the QueryService daemon')
    parser.add_argument('--config', type=str, required=True,
                        help='service configuration file')
    parser.add_argument('--debug', action='store_true',
                        help='Enable web server DEBUG mode')
    parser.add_argument('--pid-file', type=str, help='PID file for the queryservice daemon',
                        default='/tmp/pyehr_queryservice.pid')
    parser.add_argument('--log-file', type=str, help='LOG file (default=stderr)')
    parser.add_argument('--log-level', type=str, default='INFO',
                        help='LOG level (default=INFO)')
    return parser


def main(argv):
    parser = get_parser()
    args = parser.parse_args(argv)
    logger = get_logger('query_service_daemon_main')
    conf = get_service_configuration(args.config, logger)
    if not conf:
        msg = 'It was impossible to load configuration, exit'
        logger.critical(msg)
        sys.exit(msg)
    qservice = QueryService(log_file=args.log_file, log_level=args.log_level,
                            **conf.get_db_configuration())
    qservice.add_index_service(**conf.get_index_configuration())
    check_pid_file(args.pid_file, logger)
    create_pid(args.pid_file)
    qservice.start_service(debug=args.debug, **conf.get_query_service_configuration())
    destroy_pid(args.pid_file)


if __name__ == '__main__':
    main(sys.argv[1:])
