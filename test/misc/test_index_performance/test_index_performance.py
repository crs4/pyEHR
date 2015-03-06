import sys,argparse
from lxml import etree
from random import randint
from copy import deepcopy
from pyehr.ehr.services.dbmanager.dbservices import DBServices
from pyehr.ehr.services.dbmanager.dbservices.wrappers import ArchetypeInstance,\
    ClinicalRecord, PatientRecord
from pyehr.utils.services import get_service_configuration, get_logger
from pyehr.aql.parser import Parser
from pyehr.ehr.services.dbmanager.errors import IndexServiceConnectionError
from pyehr.ehr.services.dbmanager.errors import UnknownDriverError

class TestIndexPerformance(object):

    def __init__(self, pyehr_conf, xml_file, patients_batch_size,
                 min_ehr_for_patient, max_ehr_for_patient, log_level,
                 log_file):
        self.logger = get_logger('test_index_performance', log_level=log_level,
                                 log_file=log_file)
        conf = get_service_configuration(pyehr_conf, logger=self.logger)
        self.dbs = DBServices(logger=self.logger, **conf.get_db_configuration())
        self.dbs.set_index_service(**conf.get_index_configuration())
        self.patients_batch_size = patients_batch_size
        self.min_ehr_for_patient = min_ehr_for_patient
        self.max_ehr_for_patient = max_ehr_for_patient
        self.master_archetype = etree.parse(xml_file)

    def build_record(self, xml_doc):
        archetype = ArchetypeInstance(xml_doc.get('class'), {})
        for i, child in enumerate(xml_doc.iterchildren()):
            if randint(0, 1) == 1:
                archetype.archetype_details['at%04d' % i] = self.build_record(child)
        return archetype

    def update_counter(self, archetype, hits_counter):
        n = hits_counter.xpath('//archetype[@class="%s"]' % archetype.archetype_class)[0]
        if n.get('hits_count'):
            n.set('hits_count', '%d' % (int(n.get('hits_count')) + 1))
        else:
            n.set('hits_count', "1")
        for v in archetype.archetype_details.itervalues():
            if isinstance(v, ArchetypeInstance):
                self.update_counter(v, hits_counter)

    def create_dataset(self):
        self.logger.info('*** Creating test dataset ***')
        hits_counter = deepcopy(self.master_archetype)
        for x in xrange(self.patients_batch_size):
            p = self.dbs.save_patient(PatientRecord('PATIENT_%d' % x))
            self.logger.debug('Saved patient %s', p.record_id)
            for y in xrange(randint(self.min_ehr_for_patient, self.max_ehr_for_patient)):
                a = self.build_record(self.master_archetype.getroot())
                crec, p = self.dbs.save_ehr_record(ClinicalRecord(a), p)
                self.update_counter(crec.ehr_data, hits_counter)
                self.logger.debug('-- Saved EHR with ID %s', crec.record_id)
        self.logger.info('*** Dataset created ***')
        return hits_counter

    def extract_aql_contains(self, xml_node, ancestors=[]):
        paths = []
        node_path = ancestors + [xml_node.get('class')]
        paths.append(node_path)
        for c in xml_node.iterchildren():
            paths.extend(self.extract_aql_contains(c, node_path))
        return paths

    def build_aql_query(self, containment_path):
        query = 'SELECT oa/data FROM Ehr [] '
        for i, path in enumerate(containment_path):
            query += 'CONTAINS Observation o%s[%s] ' % (chr(97 + i), path)
        return query

    def check_results(self, results_count, hits_counter, node_class):
        hc_node = hits_counter.xpath('//archetype[@class="%s"]' % node_class)[0]
        try:
            return results_count == int(hc_node.get('hits_count'))
        except TypeError:
            return 0 == results_count

    def run(self):
        hits_counter = self.create_dataset()
        self.logger.info('*** Building paths for AQL queries ***')
        paths = self.extract_aql_contains(self.master_archetype.getroot())
        self.logger.info('*** Building paths for AQL queries ***')
        parser = Parser()
        for p in paths:
            self.logger.info('-- Executing query')
            aql = self.build_aql_query(p)
            qm = parser.parse(aql)
            indices = self.dbs.index_service.get_matching_ids(qm.location.containers)
            if self.dbs.driver == 'mongodb':
                q = {'ehr_structure_id': {'$in': indices}}
            elif self.dbs.driver == 'elasticsearch':
                indices_elastic = []
                for ind in indices:
                    indices_elastic.append(ind.lower())
                q = {"query": { "terms" : {"ehr_structure_id":indices_elastic}}}
            else:
                raise UnknownDriverError('Unknown driver: %s' % self.driver)
            drf = self.dbs._get_drivers_factory(self.dbs.ehr_repository)
            with drf.get_driver() as driver:
                results = list(driver.get_records_by_query(q))
                try:
                    self.logger.debug('First result: %r', results[0])
                except IndexError:
                    pass
                self.logger.info('-- Executed query: %s', aql)
                self.logger.info('---- COUNTER CHECK STATUS (found %d records): %s', len(results),
                                 self.check_results(len(results), hits_counter, p[-1]))

    def cleanup(self, index_cleanup=True):
        self.logger.info('*** Deleting pyEHR data ***')
        for p in self.dbs.get_patients(fetch_ehr_records=False):
            self.logger.debug('-- Deleting patient %s and EHRs', p.record_id)
            self.dbs.delete_patient(p, cascade_delete=True)
        if index_cleanup:
            self.logger.info('*** Deleting index data ***')
            self.dbs.index_service.connect()
            self.dbs.index_service.session.execute('drop database %s' % self.dbs.index_service.db)
            self.dbs.index_service.disconnect()


def get_parser():
    parser = argparse.ArgumentParser('Run a performance test for the IndexService')
    parser.add_argument('--conf-file', type=str, required=True,
                        help='pyEHR configuration file')
    parser.add_argument('--xml-def-file', type=str, required=True,
                        help='An XML file containing the full definition for the Archetype structure')
    parser.add_argument('--batch-size', type=int, default=1000,
                        help='The size of the batch of Patients that are going to be generated (default value is 1000')
    parser.add_argument('--max-ehr-for-patient', type=int, default=10,
                        help='The max amount of EHR that can be saved for each patient (default is 10)')
    parser.add_argument('--min-ehr-for-patient', type=int, default=1,
                        help='The min amount of EHR that can be saved for each patient (default is 1)')
    parser.add_argument('--cleanup', action='store_true',
                        help='Clean data when job is completed')
    parser.add_argument('--loglevel', default='INFO', type=str, help='logging level',
                        choices=['INFO', 'DEBUG', 'ERROR', 'WARNING', 'CRITICAL'])
    parser.add_argument('--logfile', type=str, help='log file (default=stderr)')
    return parser


def main(argv):
    parser = get_parser()
    args = parser.parse_args(argv)
    test = TestIndexPerformance(args.conf_file, args.xml_def_file, args.batch_size,
                                args.min_ehr_for_patient, args.max_ehr_for_patient,
                                args.loglevel, args.logfile)
    try:
        test.run()
    except IndexServiceConnectionError, isce:
        test.cleanup(index_cleanup=False)
        raise isce
    if args.cleanup:
        test.cleanup()


if __name__ == '__main__':
    main(sys.argv[1:])