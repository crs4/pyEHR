import sys, argparse, json
from functools import wraps

from bottle import post, get, put, delete, run, response, request,\
    abort, HTTPError

from pyehr.utils import get_logger
from pyehr.utils.services import get_service_configuration, check_pid_file,\
    create_pid, destroy_pid, get_rotating_file_logger
from pyehr.ehr.services.dbmanager.dbservices import DBServices
from pyehr.ehr.services.dbmanager.dbservices.wrappers import PatientRecord,\
    ClinicalRecord, ArchetypeInstance
import pyehr.ehr.services.dbmanager.errors as pyehr_errors


class DBService(object):

    def __init__(self, driver, host, database, versioning_database=None,
                 patients_repository=None, ehr_repository=None,
                 ehr_versioning_repository=None,
                 port=None, user=None, passwd=None,
                 log_file=None, log_level='INFO'):
        if not log_file:
            self.logger = get_logger('db_service_daemon', log_level=log_level)
        else:
            self.logger = get_rotating_file_logger('db_service_daemon', log_file,
                                                   log_level=log_level)
        self.dbs = DBServices(driver, host, database, versioning_database,
                              patients_repository, ehr_repository,
                              ehr_versioning_repository,
                              port, user, passwd, self.logger)
        #######################################################
        # Web Service methods
        #######################################################
        # ---- PUT METHODS ----
        put('/patient')(self.save_patient)
        put('/ehr')(self.save_ehr_record)
        put('/batch/save/patient')(self.batch_save_patient)
        put('/batch/save/patients')(self.batch_save_patients)
        # ---- DELETE METHODS ----
        delete('/patient/<patient_id>/delete')(self.delete_patient)
        delete('/ehr/<patient_id>/<ehr_record_id>/delete')(self.delete_ehr_record)
        delete('/patient/<patient_id>/hide')(self.hide_patient)
        delete('/ehr/<patient_id>/<ehr_record_id>/hide')(self.hide_ehr_record)
        # ---- GET METHODS -----
        get('/patient/<patient_id>')(self.get_patient)
        get('/patient/load_ehr_records')(self.load_ehr_records)
        get('/ehr/<patient_id>/<ehr_record_id>')(self.get_ehr_record)
        # ---- POST METHODS ----
        post('/ehr/update')(self.update_ehr_record)
        # ---- UTILITIES ----
        post('/check/status/dbservice')(self.test_server)
        get('/check/status/dbservice')(self.test_server)

    def add_index_service(self, url, database, user, passwd):
        self.dbs.set_index_service(url, database, user, passwd)

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
                # if an abort was called in wrapped function, raise the generated HTTPError
                raise
            except Exception, e:
                import traceback
                inst.logger.error(traceback.print_stack())
                msg = 'Unexpected error: %s' % e.message
                inst._error(msg, 500)
        return wrapper

    def _error(self, msg, error_code=500):
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

    def _get_bool(self, str_val):
        if str_val is None:
            return None
        elif str_val.upper() == 'TRUE':
            return True
        elif str_val.upper() == 'FALSE':
            return False
        else:
            raise ValueError('Can\'t convert to boolean value %s' % str_val)

    def test_server(self):
        return 'DBService daemon running'

    @exceptions_handler
    def save_patient(self):
        """
        Create a new PatientRecord from the given values. Returns the saved record in
        its JSON encoding
        """
        params = request.json
        try:
            record_id = params.get('patient_id')
            if not record_id:
                self._missing_mandatory_field('patient_id')
            patient_record_conf = {
                'record_id': record_id,
            }
            if params.get('creation_time'):
                patient_record_conf['creation_time'] = params.get('creation_time')
            if params.get('active') is not None:
                patient_record_conf['active'] = params.get('active')
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
        params = request.json
        try:
            patient_id = params.get('patient_id')
            if not patient_id:
                self._missing_mandatory_field('patient_id')
            ehr_record_conf = params.get('ehr_record')
            self.logger.debug('EHR data: %r', ehr_record_conf)
            if not ehr_record_conf:
                self._missing_mandatory_field('ehr_record')
            active = params.get('active')
            if active is None:
                active = True
            creation_time = params.get('creation_time')
            record_id = params.get('record_id')
            arch_details = ArchetypeInstance.from_json(json.loads(ehr_record_conf))
            ehr_record = ClinicalRecord(arch_details, creation_time=creation_time,
                                        active=active, record_id=record_id)
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
            #TODO: check this, not quite sure about the 400 error code...
            self._error(str(ve), 400)

    @exceptions_handler
    def delete_patient(self, patient_id):
        params = request.json
        try:
            cascade_delete = params.get('cascade_delete')
            if cascade_delete is None:
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
    def delete_ehr_record(self, patient_id, ehr_record_id):
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
    def hide_patient(self, patient_id):
        """
        Hide a patient record and related EHR records
        """
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
    def hide_ehr_record(self, patient_id, ehr_record_id):
        """
        Hide an EHR record by ID
        """
        patient_record = self.dbs.get_patient(patient_id, fetch_ehr_records=False)
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
                'MESSAGE': 'EHR record with ID %s is not connected to patient record or is already an hidden record' %
                           ehr_record_id
            }
        else:
            self.dbs.hide_ehr_record(ehr_record)
            response_body = {
                'SUCCESS': True,
                'MESSAGE': 'EHR record with ID %s successfully hidden' % ehr_record_id
            }
        return self._success(response_body)

    @exceptions_handler
    def get_patient(self, patient_id):
        params = request.params
        if params:
            fetch_ehr = self._get_bool(params.get('fetch_ehr_records'))
            print 'fetch_ehr_records is %r' % fetch_ehr
            if fetch_ehr is None:
                fetch_ehr = True
            fetch_hidden_ehr = self._get_bool(params.get('fetch_hidden_ehr_records'))
            if fetch_hidden_ehr is None:
                fetch_hidden_ehr = False
        else:
            fetch_ehr = True
            fetch_hidden_ehr = False
        patient_record = self.dbs.get_patient(patient_id, fetch_ehr,
                                              fetch_hidden_ehr)
        response_body = {'SUCCESS': True}
        if not patient_record:
            response_body['RECORD'] = None
        else:
            response_body['RECORD'] = patient_record.to_json()
        return self._success(response_body)

    @exceptions_handler
    def get_ehr_record(self, patient_id, ehr_record_id):
        ehr_record = self.dbs.get_ehr_record(ehr_record_id, patient_id)
        response_body = {'SUCCESS': True}
        if not ehr_record:
            response_body['RECORD'] = None
        else:
            response_body['RECORD'] = ehr_record.to_json()
        return self._success(response_body)

    @exceptions_handler
    def load_ehr_records(self):
        """
        Load EHR records data for a given PatientRecord (in JSON format), this method is usefull
        if the PatientRecord was retrieved with the fetch_ehr_records flag set up to False.
        Only the ClinicalRecords (in JSON format) embedded in the PatientRecord will be loaded,
        other records connected to the given PatientRecord will be ignored.
        """
        params = request.forms
        try:
            patient_record = params.get('patient_record')
            if not patient_record:
                self._missing_mandatory_field('patient_record')
            patient_record = PatientRecord.from_json(json.loads(patient_record))
            patient_record = self.dbs.load_ehr_records(patient_record)
            response_body = {
                'SUCCESS': True,
                'RECORD': patient_record.to_json()
            }
            return self._success(response_body)
        except pyehr_errors.InvalidJsonStructureError:
            msg = 'Invalid PatientRecord JSON structure'
            self._error(msg, 500)

    def _save_patient_from_batch(self, patient_record, ignore_duplicated_ehr=False):
        """
        Returns SUCCESS_STATE, ERROR_MESSAGE, SAVED_RECORD, ERROR_RECORDS
        """
        ehr_records = patient_record.ehr_records
        patient_record.ehr_records = []
        patient_record = self.dbs.save_patient(patient_record)
        try:
            _, patient_record, errors = self.dbs.save_ehr_records(ehr_records, patient_record,
                                                                  ignore_duplicated_ehr)
        except pyehr_errors.DuplicatedKeyError, dup_key_err:
            msg = dup_key_err.message
            self.dbs.delete_patient(patient_record, cascade_delete=True)
            return False, msg, None, None
        return True, None, patient_record, errors

    @exceptions_handler
    def batch_save_patient(self):
        """
        Save a PatientRecord and one or more connected ClinicalRecord at the same time.
        If one of the EHR records can't be saved, all saved records (patient and ehr data
        within this batch) will be deleted.
        """
        params = request.json
        try:
            patient_data = params.get('patient_data')
            if patient_data is None:
                self._missing_mandatory_field('patient_data')
            patient_record = PatientRecord.from_json(patient_data)
            success, msg, patient_record, errors = self._save_patient_from_batch(patient_record)
            if success:
                response_body = {
                    'SUCCESS': True,
                    'RECORD': patient_record.to_json()
                }
                return self._success(response_body)
            else:
                self._error(msg, 500)
        except pyehr_errors.DuplicatedKeyError:
            msg = 'Duplicated key error for PatientRecord with ID %s' % patient_record.record_id
            self._error(msg, 500)
        except pyehr_errors.InvalidJsonStructureError, je:
            self._error(str(je), 500)
        except ValueError, ve:
            # TODO: check this, not quite sure about the 400 error code...
            self._error(str(ve), 400)

    @exceptions_handler
    def batch_save_patients(self):
        """
        Save a list of PatientRecords and connected ClinicalRecords at the same time.
        For each PatientRecord, if an error occurs during the saving procedure data for that
        specific patient will be delete (patient data + ehr records).
        Two lists of JSON records will be returned, one with the saved records and one with
        the records that failes with the related error.
        """
        params = request.json
        patients_data = params.get('patients_data')
        if patients_data is None:
            self._missing_mandatory_field('patients_data')
        response_body = {
            'SUCCESS': True,
            'SAVED': [],
            'ERRORS': []
        }
        try:
            for patient in patients_data:
                try:
                    patient_record = PatientRecord.from_json(patient)
                    success, msg, patient_record, errors = self._save_patient_from_batch(patient_record)
                    if success:
                        response_body['SAVED'].append(patient_record.to_json())
                    else:
                        response_body['ERRORS'].append({'MESSAGE': msg, 'RECORD': patient})
                except pyehr_errors.DuplicatedKeyError:
                    msg = 'Duplicated key error for PatientRecord with ID %s' % patient_record.record_id
                    response_body['ERRORS'].append({'MESSAGE': msg, 'RECORD': patient})
                except pyehr_errors.InvalidJsonStructureError, je:
                    response_body['ERRORS'].append({'MESSAGE': str(je), 'RECORD': patient})
            return self._success(response_body)
        except ValueError, ve:
            # TODO: check this, not quite sure about the 400 error code...
            self._error(str(ve), 400)

    @exceptions_handler
    def update_ehr_record(self):
        """
        Update an existing EHR record with the version given in the "ehr_record" field
        of JSON parameters of the POST request.
        """
        params = request.json
        response_body = None
        try:
            ehr_record = params.get('ehr_record')
            if ehr_record is None:
                self._missing_mandatory_field('ehr_record')
            ehr_record = ClinicalRecord.from_json(json.loads(ehr_record))
            ehr_record = self.dbs.update_ehr_record(ehr_record)
            response_body = {
                'SUCCESS': True,
                'RECORD': ehr_record.to_json()
            }
        except pyehr_errors.InvalidJsonStructureError:
            msg = 'Invalid Clinical Record record, unable to save'
            self._error(msg, 500)
        except pyehr_errors.RedundantUpdateError:
            response_body = {
                'MESSAGE': 'Redundant update, old and new records are identical',
                'SUCCESS': False
            }
        except pyehr_errors.OptimisticLockError, opt_locl_err:
            response_body = {
                'MESSAGE': opt_locl_err.message,
                'SUCCESS': False
            }
        except pyehr_errors.OperationNotAllowedError, op_na_err:
            msg = op_na_err.message
            self._error(msg, 500)
        finally:
            if response_body:
                return self._success(response_body)

    def start_service(self, host, port, engine, debug=False):
        self.logger.info('Starting DBService daemon with DEBUG set to %s', debug)
        try:
            run(host=host, port=port, server=engine, debug=debug)
        except Exception, e:
            self.logger.critical('An error has occurred: %s', e)


def get_parser():
    parser = argparse.ArgumentParser('Run the DBService daemon')
    parser.add_argument('--config', type=str, required=True,
                        help='service configuration file')
    parser.add_argument('--debug', action='store_true',
                        help='Enable web server DEBUG mode')
    parser.add_argument('--pid-file', type=str, help='PID file for the dbservice daemon',
                        default='/tmp/pyehr_dbservice.pid')
    parser.add_argument('--log-file', type=str, help='LOG file (default=stderr)')
    parser.add_argument('--log-level', type=str, default='INFO',
                        help='LOG level (default=INFO)')
    return parser


def main(argv):
    parser = get_parser()
    args = parser.parse_args(argv)
    logger = get_logger('db_service_daemon_main')
    conf = get_service_configuration(args.config, logger)
    if not conf:
        msg = 'It was impossible to load configuration, exit'
        logger.critical(msg)
        sys.exit(msg)
    dbs = DBService(log_file=args.log_file, log_level=args.log_level,
                    **conf.get_db_configuration())
    dbs.add_index_service(**conf.get_index_configuration())
    check_pid_file(args.pid_file, logger)
    create_pid(args.pid_file)
    dbs.start_service(debug=args.debug, **conf.get_db_service_configuration())
    destroy_pid(args.pid_file)


if __name__ == '__main__':
    main(sys.argv[1:])
