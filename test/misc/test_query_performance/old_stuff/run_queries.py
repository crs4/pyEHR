import csv
import time
from functools import wraps

from pyehr.ehr.services.dbmanager.querymanager import QueryManager
from pyehr.utils.services import get_service_configuration, get_logger

def get_execution_time(f):
    @wraps(f)
    def wrapper(inst, *args, **kwargs):
        start_time = time.time()
        res = f(inst, *args, **kwargs)
        execution_time = time.time() - start_time
        return res, execution_time
    return wrapper

@get_execution_time
def run_query(query, query_manager):
    return query_manager.execute_aql_query(query, None)

cfg = get_service_configuration('/home/luca/work/pyEHR/config/bruja.mongodb.conf')
dbcfg = cfg.get_db_configuration()
dbcfg['database'] = 'pyehr_d-4_w-10_str-2000'
icfg = cfg.get_index_configuration()
icfg['database'] = 'pyehr_index_d-4_w-10_str-2000'

logger = get_logger('index_service', log_level='DEBUG')

query_manager = QueryManager(**dbcfg)
query_manager.set_index_service(**icfg)

query_manager.index_service.logger = logger

queries = []
with open('query_file') as f:
    q = ""
    for line in f.readlines():
        if line == "\n":
            queries.append(q.replace("\n", " "))
            q = ""
        else:
            q += line

shards = 10

with open('query_time_%dshards.tsv' % shards, 'w') as ofile:
    header = ['deep_1', 'deep_1+w', 'deep_2', 'deep_2+w',
              'deep_3', 'deep_3+w', 'deep_4', 'deep_4+w']
    writer = csv.DictWriter(ofile, ['cluster_nodes'] + header, delimiter='\t')
    writer.writeheader()
    row = {'cluster_nodes': shards}
    for i, q in enumerate(queries):
        csv_index = i % 8
        print q
        res, t = run_query(q, query_manager)
        print res.total_results, t
        row[header[csv_index]] = t
        if csv_index == 7:
            print row
            writer.writerow(row)
            row = {'cluster_nodes': shards}

