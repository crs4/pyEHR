import sys, argparse, json
from functools import wraps

from bottle import post, run, Response, request, abort, HTTPError

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
        post('/patient/add')(self.save_patient)
        post('/patient/hide')(self.hide_patient)
        post('/patient/delete')(self.delete_patient)
        post('/patient/get')(self.get_patient)
        post('/ehr/add')(self.save_ehr_record)
        post('/ehr/delete')(self.delete_ehr_record)

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
        params = request.forms
        try:
            record_id = params.get('patient_id')
            if not record_id:
                msg = 'Missing record ID, cannot create a patient record'
                self._error(msg, 400)
            patient_record_conf = {
                'record_id': record_id,
            }
            if params.get('creation_time'):
                patient_record_conf['creation_time'] = float(params.get('creation_time'))
            if params.get('active'):
                patient_record_conf['active'] = self._get_bool(params.get('active'))
            patient_record = PatientRecord(**patient_record_conf)
            patient_record = self.dbs.save_patient(patient_record)
            response_body = {
                'SUCCESS': True,
                'RECORD': patient_record.to_json()
            }
            return self._success(response_body)
        except pyehr_errors.DuplicatedKeyError:
            msg = 'Duplicated key error for PatientRecord with ID %s' % patient_record.record_id
            self._error(msg, 500)
        except pyehr_errors.InvalidRecordTypeError:
            msg = 'Invalid PatientRecord, unable to save'
            self._error(msg, 500)
        except ValueError, ve:
            self._error(str(ve), 500)

    @exceptions_handler
    def save_ehr_record(self):
        """
        Save a new EHR record and link it to an existing patient record.
        EHR record must be a valid JSON dictionary.
        """
        params = request.forms
        try:
            patient_id = params.get('patient_id')
            if not patient_id:
                msg = 'Missing patient ID, cannot create a clinical record'
                self._error(msg, 400)
            ehr_record_conf = params.get('ehr_record')
            self.logger.debug('EHR data: %r', ehr_record_conf)
            if not ehr_record_conf:
                msg = 'Missing EHR data, cannot create a clinical record'
                self._error(msg, 400)
            ehr_record = ClinicalRecord.from_json(json.loads(ehr_record_conf))
            patient_record = self.dbs.get_patient(patient_id)
            if not patient_record:
                # TODO: check if an error is a better solution here
                response_body = {
                    'SUCCESS': False,
                    'ERROR': 'There is no patient record with ID %s' % patient_id
                }
                return self._success(response_body)
            ehr_record, _ = self.dbs.save_ehr_record(ehr_record, patient_record)
            response_body = {
                'SUCCESS': True,
                'RECORD': ehr_record.to_json()
            }
            return self._success(response_body)
        except pyehr_errors.InvalidJsonStructureError:
            msg = 'Invalid EHR record, unable to save'
            self._error(msg, 500)
        except pyehr_errors.DuplicatedKeyError:
            msg = 'Duplicated key error for EHR record with ID %s' % ehr_record.record_id
            self._error(msg, 500)
        except ValueError, ve:
            self._error(str(ve), 500)

    @exceptions_handler
    def delete_patient(self):
        params = request.forms
        try:
            patient_id = params.get('patient_id')
            if not patient_id:
                msg = 'Missing patient ID, cannot delete record'
                self._error(msg, 400)
            cascade_delete = params.get('cascade_delete')
            if cascade_delete:
                cascade_delete = self._get_bool(cascade_delete)
            else:
                cascade_delete = False
            patient_record = self.dbs.get_patient(patient_id)
            if not patient_record:
                # TODO: check if an error is a better solution here
                response_body = {
                    'SUCCESS': False,
                    'ERROR': 'There is no patient record with ID %s' % patient_id
                }
                return self._success(response_body)
            self.dbs.delete_patient(patient_record, cascade_delete)
            response_body = {
                'SUCCESS': True,
                'MESSAGE': 'Patient record with ID %s successfully deleted' % patient_id
            }
            return self._success(response_body)
        except pyehr_errors.CascadeDeleteError:
            msg = 'Patient record is connected to one or more EHR records, enable cascade deletion to continue'
            self._error(msg, 500)

    @exceptions_handler
    def delete_ehr_record(self):
        params = request.forms
        patient_id = params.get('patient_id')
        if not patient_id:
            msg = 'Missing patient ID, cannot delete record'
            self._error(msg, 400)
        ehr_record_id = params.get('ehr_record_id')
        if not ehr_record_id:
            msg = 'Missing EHR record ID, cannot delete record'
            self._error(msg, 400)
        patient_record = self.dbs.get_patient(patient_id, fetch_ehr_records=False,
                                              fetch_hidden_ehr=True)
        if not patient_record:
            response_body = {
                'SUCCESS': False,
                'MESSAGE': 'There is no patient record with ID %s' % patient_id
            }
            return self._success(response_body)
        ehr_record = patient_record.get_clinical_record_by_id(ehr_record_id)
        if not ehr_record:
            response_body = {
                'SUCCESS': False,
                'MESSAGE': 'Patient record %s is not connected to an EHR record with ID %s' % (patient_id,
                                                                                               ehr_record_id)
            }
        else:
            self.dbs.remove_ehr_record(ehr_record, patient_record)
            response_body = {
                'SUCCESS': True,
                'MESSAGE': 'EHR record with ID %s successfully deleted' % ehr_record_id
            }
        return self._success(response_body)

    @exceptions_handler
    def hide_patient(self):
        """
        Hide a patient record and related EHR records
        """
        params = request.forms
        patient_id = params.get('patient_id')
        if not patient_id:
            msg = 'Missing patient ID, cannot hide record'
            self._error(msg, 400)
        patient_record = self.dbs.get_patient(patient_id)
        if not patient_record:
            # TODO: check if an error is a better solution here
            response_body = {
                'SUCCESS': False,
                'ERROR': 'There is no patient record with ID %s' % patient_id
            }
            return self._success(response_body)
        patient_record = self.dbs.hide_patient(patient_record)
        response_body = {
            'SUCCESS': True,
            'RECORD': patient_record.to_json()
        }
        return self._success(response_body)

    @exceptions_handler
    def get_patient(self):
        params = request.forms
        patient_id = params.get('patient_id')
        if not patient_id:
            msg = 'Missing patient ID, cannot fetch data'
            self._error(msg, 400)
        fetch_ehr = params.get('fetch_ehr_records')
        if fetch_ehr:
            fetch_ehr = self._get_bool(fetch_ehr)
        else:
            fetch_ehr = True
        fetch_hidden_ehr = params.get('fetch_hidden_ehr_records')
        if fetch_hidden_ehr:
            fetch_hidden_ehr = self._get_bool(fetch_hidden_ehr)
        else:
            fetch_hidden_ehr = False
        patient_record = self.dbs.get_patient(patient_id, fetch_ehr,
                                              fetch_hidden_ehr)
        response_body = {'SUCCESS': True}
        if not patient_record:
            response_body['RECORD'] = None
        else:
            response_body['RECORD'] = patient_record.to_json()
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