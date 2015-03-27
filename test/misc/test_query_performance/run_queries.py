import argparse, sys, time, json, os

from pyehr.ehr.services.dbmanager.querymanager import QueryManager, Parser
from pyehr.utils.services import get_service_configuration
from pyehr.utils import get_logger, decode_dict


def get_parser():
    parser = argparse.ArgumentParser('Run queries and measure execution times')
    parser.add_argument('--queries_file', type=str, required=True,
                        help='The JSON file with queries definitions')
    parser.add_argument('--pyehr_config', type=str, required=True,
                        help='pyEHR config file')
    parser.add_argument('--results_file', type=str, required=True,
                        help='The output file where results and times will be reported')
    parser.add_argument('--query_processes', type=int, default=1,
                        help='The number of parallel processes used to run each query (default is single-process)')
    parser.add_argument('--fetch_threshold', type=int, default=10000,
                        help='Fetch only result sets whose size is lesser or equal to this value (default 10000)')
    parser.add_argument('--log_file', type=str, help='LOG file (default=stderr)')
    parser.add_argument('--log_level', type=str, default='INFO',
                        help='LOG level (default=INFO)')
    return parser


def get_query_manager(conf_file):
    cfg = get_service_configuration(conf_file)
    dbcfg = cfg.get_db_configuration()
    icfg = cfg.get_index_configuration()
    qm = QueryManager(**dbcfg)
    qm.set_index_service(**icfg)
    return qm


def load_queries(queries_file):
    with open(queries_file) as f:
        queries = decode_dict(json.loads(f.read()))
    for q, conf in queries.iteritems():
        if isinstance(conf['query'], list):
            conf['query'] = ' '.join(conf['query'])
    return queries


def run_query(qmanager, query, query_processes, count_only, logger):
    logger.info('QUERY PROCESSES: %d --- COUNT ONLY: %s' % (query_processes, count_only))
    start_time = time.time()
    results = qmanager.execute_aql_query(query, None, count_only, query_processes)
    execution_time = time.time() - start_time
    if count_only:
        logger.info('Query executed in %f seconds' % execution_time)
    else:
        logger.info('Query executed in %f seconds, retrieved %d results' %
                    (execution_time, results.total_results))
    return results, execution_time


def get_index_service_time(qmanager, query, logger):
    query_parser = Parser()
    query_model = query_parser.parse(query)
    drf = qmanager._get_drivers_factory(qmanager.ehr_repository)
    with drf.get_driver() as driver:
        start_time = time.time()
        _, _ = driver.index_service.map_aql_contains(query_model.location.containers)
        index_time = time.time() - start_time
    logger.info('Index time search took %f seconds' % index_time)
    return index_time


def get_expected_results_count(qmanager, expected_results_percentage, logger):
    drf = qmanager._get_drivers_factory(qmanager.ehr_repository)
    with drf.get_driver() as driver:
        total_records_count = driver.documents_count
    logger.info('Collection %s contains %d records' % (qmanager.ehr_repository,
                                                       total_records_count))
    return int(round((expected_results_percentage * total_records_count) / 100.))


def save_report(report_file, results_map):
    with open(report_file, 'w') as f:
        for query in sorted(results_map):
            f.write(json.dumps({query: results_map[query]}) + os.linesep)


def main(argv):
    parser = get_parser()
    args = parser.parse_args(argv)
    logger = get_logger('run_queries', log_level=args.log_level, log_file=args.log_file)

    query_manager = get_query_manager(args.pyehr_config)
    logger.info('Loading queries from file %s' % args.queries_file)
    queries = load_queries(args.queries_file)
    logger.info('Loaded %d queries' % len(queries))

    results_map = dict()
    for query_label, query_conf in sorted(queries.iteritems()):
        expected_results = get_expected_results_count(query_manager,
                                                      query_conf['expected_results_percentage'],
                                                      logger)
        logger.info('Running query "%s" (expected results: %d)' % (query_label, expected_results))
        count_results, count_exec_time = run_query(query_manager, query_conf['query'],
                                                   args.query_processes, True, logger)
        if count_results <= args.fetch_threshold:
            fetch_results, fetch_exec_time = run_query(query_manager, query_conf['query'],
                                                       args.query_processes, False, logger)
        else:
            fetch_results, fetch_exec_time = None, None
        if fetch_results:
            # safety check
            assert count_results == fetch_results.total_results
        index_time = get_index_service_time(query_manager, query_conf['query'], logger)
        results_map[query_label] = {
            'execution_time': {
                'count': count_exec_time,
                'fetch': fetch_exec_time
            },
            'index_service_time': index_time,
            'query_results_count': count_results,
            'expected_results_count': expected_results
        }
        if count_results != expected_results:
            logger.warning('Retrieved %d results, expected %s' % (count_results, expected_results))

    logger.info('Writing output file %s' % args.results_file)
    save_report(args.results_file, results_map)

if __name__ == '__main__':
    main(sys.argv[1:])