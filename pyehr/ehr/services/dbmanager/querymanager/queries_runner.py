import multiprocessing

from pyehr.ehr.services.dbmanager.querymanager import QueryManager

from pyehr.utils.services import get_logger


class QueryProcess(multiprocessing.Process):

    def __init__(self, query_manager_conf, index_service_conf, query_label,
                 aql_query, results_queue, logger=None):
        multiprocessing.Process.__init__(self)
        self.query_manager = QueryManager(**query_manager_conf)
        self.query_manager.set_index_service(**index_service_conf)
        self.query_label = query_label
        self.query = aql_query
        self.queue = results_queue
        if logger:
            self.logger = logger
        else:
            self.logger = get_logger('query_thread')

    def run(self):
        self.logger.debug('Start thread for query %s', self.query_label)
        res = self.query_manager.execute_aql_query(self.query)
        self.logger.debug('Retrieved %d results', res.total_results)
        self.queue.put({'query': self.query_label, 'results': res})


class QueriesRunner(object):

    def __init__(self, query_manager_conf, index_sevice_conf, logger=None):
        self.qm_conf = query_manager_conf
        self.idxs_conf = index_sevice_conf
        self.queries = dict()
        self.queries_results = dict()
        if logger:
            self.logger = logger
        else:
            self.logger = get_logger('queries_runner')

    @property
    def queries_count(self):
        return len(self.queries)

    @property
    def results_count(self):
        return len(self.queries_results)

    def add_query(self, query_label, aql_query):
        if not query_label in self.queries:
            self.queries[query_label] = aql_query
        else:
            raise KeyError('Query label %s already in use' % query_label)

    def execute_queries(self):
        results_queue = multiprocessing.Queue(len(self.queries))
        query_threads = list()
        self.logger.debug('Start processing queues')
        for label, query in self.queries.iteritems():
            qp = QueryProcess(self.qm_conf, self.idxs_conf, label, query,
                              results_queue, self.logger)
            qp.start()
            query_threads.append(qp)
        for qp in query_threads:
            qp.join()
        self.logger.debug('Queries executed, collecting results')
        while not results_queue.empty():
            q_element = results_queue.get()
            self.queries_results.update({q_element['query']: q_element['results']})
        self.logger.debug('Results collected')

    def cleanup(self):
        self.queries = dict()
        self.queries_results = dict()

    def remove_query(self, query_label):
        try:
            del(self.queries[query_label])
        except KeyError:
            raise KeyError('There is no query labeled %s' % query_label)
        try:
            del(self.queries_results[query_label])
        except KeyError:
            pass

    def get_result_set(self, query_label):
        return self.queries_results.get(query_label)

    def get_intersection(self, field, *query_labels):
        res = set([r[field] for r in self.queries_results[query_labels[0]].results])
        for label in query_labels[1:]:
            res.intersection_update(set([r[field] for r in self.queries_results[label].results]))
        return res

    def get_union(self, field, *query_labels):
        res = set([r[field] for r in self.queries_results[query_labels[0]].results])
        for label in query_labels[1:]:
            res.update(set([r[field] for r in self.queries_results[label].results]))
        return res