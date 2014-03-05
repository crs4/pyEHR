import sys, argparse, json

from bottle import route, run, Response, request, abort

from openehr.utils import get_logger
from openehr.utils.services import get_service_configuration
from openehr.ehr.services.dbmanager.dbservices import DBServices
from openehr.ehr.services.dbmanager.dbservices.wrappers import PatientRecord, ClinicalRecord
import openehr.ehr.services.dbmanager.errors as pyehr_errors


class DBService(object):

    def __init__(self, driver, host, database, patients_repository=None,
                 ehr_repository=None, port=None, user=None, passwd=None):
        self.logger = get_logger('db_service_daemon')
        self.dbs = DBServices(driver, host, database, patients_repository,
                              ehr_repository, port, user, passwd, self.logger)
        #######################################################
        # Web Service methods
        #######################################################
        route('/add_patient')(self.save_patient)

    def _error(self, msg, error_code):
        self.logger.error(msg)
        body = {
            'SUCCESS': False,
            'MESSAGE': msg
        }
        abort(error_code, json.dumps(body))

    def _success(self, body, return_code=200):
        return Response(json.dumps(body), return_code)

    def _get_bool(self, str_val):
        if str_val.upper() == 'TRUE':
            return True
        elif str_val.upper() == 'FALSE':
            return False
        else:
            raise ValueError('Can\'t convert to boolean value %s' % str_val)

    def save_patient(self):
        """
        Create a new PatientRecord from the given values. Returns the saved record in
        its JSON encoding
        """
        params = request.query
        try:
            record_id = params.get('patient_id')
            if not record_id:
                msg = 'Missing record ID, cannot create a patient record'
                return self._error(msg, 500)
            prec_conf = {
                'record_id': record_id,
            }
            if params.get('creation_time'):
                prec_conf['creation_time'] = float(params.get('creation_time'))
            if params.get('active'):
                prec_conf['active'] = self._get_bool(params.get('active'))
            prec = PatientRecord(**prec_conf)
            prec = self.dbs.save_patient(prec)
        except pyehr_errors.DBManagerNotConnectedError:
            msg = 'Unable to connect to backend engine'
            return self._error(msg, 500)
        except pyehr_errors.DuplicatedKeyError:
            msg = 'Duplicated key error for PatientRecord with ID %s' % prec.record_id
            return self._error(msg, 500)
        except pyehr_errors.InvalidRecordTypeError:
            msg = 'Invalid PatientRecord, unable to save'
            return self._error(msg, 500)
        except pyehr_errors.UnknownDriverError, ude:
            return self._error(str(ude), 500)
        except ValueError, ve:
            return self._error(str(ve), 500)
        except Exception, e:
            msg = 'Unexpected error: %s' % e.message
            return self._error(msg, 500)
        response_body = {
            'SUCCESS': True,
            'RECORD': prec.to_json()
        }
        return self._success(response_body)

    def start_service(self, host, port, debug=False):
        self.logger.info('Starting DBService daemon with DEBUG set to %s', debug)
        try:
            run(host=host, port=port, debug=debug)
        except Exception, e:
            self.logger.critical('An errore has occurred: %s', e)


def get_parser():
    parser = argparse.ArgumentParser('Run the DBService daemon')
    parser.add_argument('--config', type=str, required=True,
                        help='service configuration file')
    parser.add_argument('--debug', action='store_true',
                        help='Enable web server DEBUG mode')
    return parser


def main(argv):
    parser = get_parser()
    args = parser.parse_args(argv)
    conf = get_service_configuration(args.config, get_logger('db_service_daemon_main'))
    if not conf:
        msg = 'It was impossible to load configuration, exit'
        sys.exit(msg)
    dbs = DBService(**conf.get_db_configuration())
    dbs.start_service(debug=args.debug, **conf.get_service_configuration())


if __name__ == '__main__':
    main(sys.argv[1:])