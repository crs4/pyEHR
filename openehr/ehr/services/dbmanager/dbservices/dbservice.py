import sys, argparse, json
from functools import wraps

from bottle import route, run, Response, request, abort, HTTPError

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
        route('/patient/add')(self.save_patient)
        route('/patient/hide')(self.hide_patient)

    def exceptions_handler(f):
        @wraps(f)
        def wrapper(inst, *args, **kwargs):
            try:
                return f(inst, *args, **kwargs)
            except pyehr_errors.DBManagerNotConnectedError:
                msg = 'Unable to connect to backend engine'
                inst._error(msg, 500)
            except pyehr_errors.UnknownDriverError, ude:
                inst._error(str(ude), 500)
            except HTTPError:
                #if an abort was called in wrapped function, raise the generated HTTPError
                raise
            except Exception, e:
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

    def _success(self, body, return_code=200):
        return Response(json.dumps(body), return_code)

    def _get_bool(self, str_val):
        if str_val.upper() == 'TRUE':
            return True
        elif str_val.upper() == 'FALSE':
            return False
        else:
            raise ValueError('Can\'t convert to boolean value %s' % str_val)

    @exceptions_handler
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
                self._error(msg, 500)
            prec_conf = {
                'record_id': record_id,
            }
            if params.get('creation_time'):
                prec_conf['creation_time'] = float(params.get('creation_time'))
            if params.get('active'):
                prec_conf['active'] = self._get_bool(params.get('active'))
            prec = PatientRecord(**prec_conf)
            prec = self.dbs.save_patient(prec)
            response_body = {
                'SUCCESS': True,
                'RECORD': prec.to_json()
            }
            return self._success(response_body)
        except pyehr_errors.DuplicatedKeyError:
            msg = 'Duplicated key error for PatientRecord with ID %s' % prec.record_id
            self._error(msg, 500)
        except pyehr_errors.InvalidRecordTypeError:
            msg = 'Invalid PatientRecord, unable to save'
            self._error(msg, 500)
        except ValueError, ve:
            self._error(str(ve), 500)

    @exceptions_handler
    def hide_patient(self):
        """
        Hide a patient record and related EHR records
        """
        params = request.query
        patient_id = params.get('patient_id')
        if not patient_id:
            msg = 'Missing patient ID, cannot hide record'
            self._error(msg, 500)
        prec = self.dbs.get_patient(patient_id)
        if not prec:
            # TODO: check if an error is a better solution here
            body = {
                'SUCCESS': False,
                'ERROR': 'There is no patient record with ID %s' % patient_id
            }
            return self._success(body)
        prec = self.dbs.hide_patient(prec)
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