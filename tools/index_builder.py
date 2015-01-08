import sys, argparse

from pyehr.ehr.services.dbmanager.dbservices import DBServices
from pyehr.utils.services import get_service_configuration
from pyehr.utils import get_logger


class IndexBuilder(object):
    def __init__(self, conf_file, db_label=None, log_file=None, log_level='INFO'):
        conf = get_service_configuration(conf_file)
        db_conf = conf.get_db_configuration()
        index_conf = conf.get_index_configuration()
        if db_label:
            db_conf['database'] = '%s_%s' % (db_conf['database'], db_label)
            index_conf['database'] = '%s_%s' % (index_conf['database'], db_label)
        self.db_service = DBServices(**db_conf)
        self.db_service.set_index_service(**index_conf)
        self.logger = get_logger('index_builder', log_file=log_file, log_level=log_level)

    def _cleanup_index(self):
        self.logger.info('Cleaning index service database')
        self.db_service.index_service.connect()
        self.db_service.index_service.basex_client.delete_database(self.db_service.index_service.db)
        self.db_service.index_service.disconnect()

    def _get_structure_ids(self):
        drf = self.db_service._get_drivers_factory(self.db_service.ehr_repository)
        with drf.get_driver() as driver:
            structure_ids = list(driver.collection.distinct('ehr_structure_id'))
            self.logger.debug('Retrieved %d structure IDs', len(structure_ids))
        return structure_ids

    def _build_entries(self, structure_ids):
        drf = self.db_service._get_drivers_factory(self.db_service.ehr_repository)
        self.logger.info('Creting new entries')
        for i, st_id in enumerate(structure_ids):
            with drf.get_driver() as driver:
                rec = driver.decode_record(driver.get_records_by_value('ehr_structure_id', st_id).next())
            arch_xml = self.db_service.index_service.get_structure(rec.ehr_data.to_json())
            _ = self.db_service.index_service.create_entry(arch_xml, st_id)
            assert _ == st_id
            self.logger.debug('Created entry for ID %s --- %d of %d', st_id, i+1, len(structure_ids))
        self.logger.info('Entries creation completed')
        self.db_service.index_service.disconnect()

    def run(self):
        self._cleanup_index()
        str_ids = self._get_structure_ids()
        self._build_entries(str_ids)


def get_parser():
    parser = argparse.ArgumentParser('Rebuild the index database for the given pyEHR environment')
    parser.add_argument('--conf-file', type=str, required=True,
                        help='pyEHR configuration file')
    parser.add_argument('--db-label', type=str, default=None,
                        help='A label that will be added to database\'s name specified in conf file')
    parser.add_argument('--log-file', type=str, help='LOG file (default=stderr)')
    parser.add_argument('--log-level', type=str, default='INFO',
                        help='LOG level (default INFO)')
    return parser


def main(argv):
    parser = get_parser()
    args = parser.parse_args(argv)
    index_builder = IndexBuilder(args.conf_file, args.db_label, args.log_file, args.log_level)
    index_builder.run()

if __name__ == '__main__':
    main(sys.argv[1:])