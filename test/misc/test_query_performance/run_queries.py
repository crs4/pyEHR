import argparse, sys, time, json, os

from pyehr.ehr.services.dbmanager.querymanager import QueryManager
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


def run_query(qmanager, query, logger):
    start_time = time.time()
    results = qmanager.execute_aql_query(query, None)
    execution_time = time.time() - start_time
    logger.info('Query executed in %f seconds, retrieved %d results' %
                (execution_time, results.total_results))
    return results, execution_time


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
        logger.info('Running query "%s"' % query_label)
        results, exec_time = run_query(query_manager, query_conf['query'], logger)
        results_map[query_label] = {
            'execution_time': exec_time,
            'query_results_count': results.total_results,
            'expected_results_count': query_conf['expected_results_count']
        }
        if results.total_results != int(query_conf['expected_results_count']):
            logger.warning('Retrieved %d results, expected %s' %
                           (results.total_results, query_conf['expected_results_count']))

    logger.info('Writing output file %s' % args.results_file)
    save_report(args.results_file, results_map)

if __name__ == '__main__':
    main(sys.argv[1:])