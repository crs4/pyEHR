import argparse, sys, time

from records_builder import get_patient_records_from_file

from pyehr.ehr.services.dbmanager.dbservices import DBServices
from pyehr.ehr.services.dbmanager.errors import DuplicatedKeyError
from pyehr.utils.services import get_service_configuration, get_logger


def get_parser():
    parser = argparse.ArgumentParser('Load patients datasets from a JSON file and populate pyEHR database')
    parser.add_argument('--datasets_file', type=str, required=True,
                        help='The file with patients datasets in JSON format')
    parser.add_argument('--compression_enabled', action='store_true',
                        help='Use if dataset file was compressed using Python\'s gzip lib')
    parser.add_argument('--pyehr_config', type=str, required=True,
                        help='pyEHR config file')
    parser.add_argument('--clean_db', action='store_true',
                        help='clean pyEHR databases before dumping new records')
    parser.add_argument('--log_file', type=str, help='LOG file (default=stderr)')
    parser.add_argument('--log_level', type=str, default='INFO',
                        help='LOG level (default=INFO)')
    return parser


def get_db_service(conf_file):
    cfg = get_service_configuration(conf_file)
    dbcfg = cfg.get_db_configuration()
    icfg = cfg.get_db_service_configuration()
    dbs = DBServices(**dbcfg)
    dbs.set_index_service(**icfg)
    return dbs


def clean_database(db_service, logger):
    logger.info('Starting cleanup')
    patients = db_service.get_patients(fetch_ehr_records=False)
    for i, p in enumerate(patients):
        logger.info('Cleaning data for patient %s (%d of %d)' % (p.record_id, i+1, len(patients)))
        db_service.delete_patient(p)
    logger.info('Cleaning index')
    db_service.index_service.connect()
    db_service.index_service.basex_client.delete_database()
    db_service.index_service.disconnect()
    logger.info('Cleanup completed')


def dump_records(dataset_file, db_service, logger):
    logger.info('Dumping records to database')
    for patient, crecs in get_patient_records_from_file(dataset_file):
        logger.info('Creating patient %s' % patient.record_id)
        try:
            patient = db_service.save_patient(patient)
        except DuplicatedKeyError:
            logger.info('Patient with ID %s already exists, adding ClinicalRecords to it' % patient.record_id)
            patient = db_service.get_patient(patient.record_id, fetch_ehr_records=False)
        logger.info('Saving %d ClinicalRecord objects' % len(crecs))
        start_time = time.time()
        db_service.save_ehr_records(crecs, patient)
        logger.info('ClinicalRecords saved in %f seconds' % (time.time() - start_time))
    logger.info('Dump completed')


def main(argv):
    parser = get_parser()
    args = parser.parse_args(argv)
    logger = get_logger('datasets_loader', log_level=args.log_level, log_file=args.log_file)

    dbservice = get_db_service(args.pyehr_config)
    if args.clean_db:
        clean_database(dbservice, logger)
    dump_records(args.datasets_file, dbservice, logger)

if __name__ == '__main__':
    main(sys.argv[1:])