import argparse, sys, time, multiprocessing, gzip

from records_builder import get_patient_records_from_file, get_patient_records_from_file_row

from pyehr.ehr.services.dbmanager.dbservices import DBServices
from pyehr.ehr.services.dbmanager.errors import DuplicatedKeyError
from pyehr.utils.services import get_service_configuration, get_logger


class DataLoaderThread(multiprocessing.Process):
    def __init__(self, db_service_conf, index_service_conf, file_row, logger):
        multiprocessing.Process.__init__(self)
        self.db_service = DBServices(**db_service_conf)
        self.db_service.set_index_service(**index_service_conf)
        self.dataset_row = file_row
        self.logger = logger

    def run(self):
        for patient, dataset in get_patient_records_from_file_row(self.dataset_row):
            self.logger.info('Saving data for patient %s' % patient.record_id)
            p = self.db_service.get_patient(patient.record_id, fetch_ehr_records=False)
            if not p:
                self.logger.info('Patient %s does not exist, creating it' % patient.record_id)
                patient = self.db_service.save_patient(patient)
            else:
                self.logger.info('Patient %s already exists, appending ClinicalRecord objects' %
                                 patient.record_id)
                patient = p
            self.logger.info('Saving %d ClinicalRecord objects (patient %s)' %
                             (len(dataset), patient.record_id))
            start_time = time.time()
            self.db_service.save_ehr_records(dataset, patient)
            self.logger.info('ClinicalRecords saved in %f seconds (patient %s)' %
                             ((time.time() - start_time), patient.record_id))


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
    parser.add_argument('--parallel_processes', type=int, default=1,
                        help='The number of parallel processes used to load data (tool is single process by default)')
    parser.add_argument('--log_file', type=str, help='LOG file (default=stderr)')
    parser.add_argument('--log_level', type=str, default='INFO',
                        help='LOG level (default=INFO)')
    return parser


def get_db_service(conf_file):
    cfg = get_service_configuration(conf_file)
    dbcfg = cfg.get_db_configuration()
    icfg = cfg.get_index_configuration()
    dbs = DBServices(**dbcfg)
    dbs.set_index_service(**icfg)
    return dbs, dbcfg, icfg


def clean_database(db_service, logger):
    logger.info('Starting cleanup')
    patients = db_service.get_patients(fetch_ehr_records=False)
    for i, p in enumerate(patients):
        logger.info('Cleaning data for patient %s (%d of %d)' % (p.record_id, i+1, len(patients)))
        db_service.delete_patient(p, cascade_delete=True)
    logger.info('Cleaning index')
    db_service.index_service.connect()
    db_service.index_service.basex_client.delete_database()
    db_service.index_service.disconnect()
    logger.info('Cleanup completed')


def dump_records(dataset_file, compressed_file, db_service, logger):
    logger.info('Dumping records to database')
    total_dump_start_time = time.time()
    for patient, crecs in get_patient_records_from_file(dataset_file, compressed_file):
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
    logger.info('Dump completed in %f seconds' % (time.time() - total_dump_start_time))


def dump_records_multiprocess(dataset_file, compressed_file, dbs_conf, index_conf,
                              max_active_processes, logger):
    logger.info('Dumping records to database')
    total_dump_start_time = time.time()
    active_processes = list()
    if compressed_file:
        f = gzip.open(dataset_file, 'rb')
    else:
        f = open(dataset_file)
    for row in f.readlines():
        proc = DataLoaderThread(dbs_conf, index_conf, row, logger)
        active_processes.append(proc)
        if len(active_processes) == max_active_processes:
            for p in active_processes:
                p.start()
            # wait until all processes completed their runs
            for p in active_processes:
                p.join()
            active_processes = list()
    # check if there are hanging processes that didn't run
    for p in active_processes:
        p.start()
    for p in active_processes:
        p.join()
    f.close()
    logger.info('Dump completed in %f seconds' % (time.time() - total_dump_start_time))


def main(argv):
    parser = get_parser()
    args = parser.parse_args(argv)
    logger = get_logger('datasets_loader', log_level=args.log_level, log_file=args.log_file)

    dbservice, dbservice_cfg, index_service_cfg = get_db_service(args.pyehr_config)
    if args.clean_db:
        clean_database(dbservice, logger)
    if args.parallel_processes == 1:
        dump_records(args.datasets_file, args.compression_enabled, dbservice, logger)
    else:
        dump_records_multiprocess(args.datasets_file, args.compression_enabled, dbservice_cfg,
                                  index_service_cfg, args.parallel_processes, logger)

if __name__ == '__main__':
    main(sys.argv[1:])