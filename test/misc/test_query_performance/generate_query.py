import sys
import argparse
import json
from collections import defaultdict
from structures_builder import build_structure, get_labels

def get_parser():
    parser = argparse.ArgumentParser('Run the test_query_performance tool')
    parser.add_argument('--conf-file', type=str, required=True,
                        help='pyEHR configuration file')
    parser.add_argument('--max-contains', type=int, required=True,
                        help='Maximum number of containments')
    parser.add_argument('--query-file', type=str, required=True,
                        help='The file with the qeuries')
    parser.add_argument('--build-queries', action="store_true", help="Build the query files")
    parser.add_argument('--results-file', type=str, help="The file with the results")

    return parser

def _get_containment(index,  structure):
    for i in xrange(index + 1):
        for k, v in structure.iteritems():
            label = k.split('-')[1]
            for x in v:
                if isinstance(x, dict):
                    structure = x

    return "CONTAINS Composition c%d[openEHR-EHR-COMPOSITION.encounter.v1.lbl-%s]" % (index, label)

def save_query(query, mode='a'):
    with open('/tmp/query_file', mode) as f:
        f.write(query + "\n")

def build_queries(max_contains):
    cts= defaultdict(list)

    leaves = {
        'bp' : "CONTAINS Observation o[openEHR-EHR-OBSERVATION.blood_pressure.v1]",
        'ua' : "CONTAINS Observation o[openEHR-EHR-OBSERVATION.urinalysis.v1]"
    }

    wheres = {
        'bp' : """WHERE o/data[at0001]/events[at0006]/data[at0003]/items[at0004]/value/magnitude >= 121\nAND o/data[at0001]/events[at0006]/data[at0003]/items[at0005]/value/magnitude >= 80""",
        'ua' : """WHERE o/data[at0001]/events[at0002]/data[at0003]/items[at0050]/value = at0120\nAND o/data[at0001]/events[at0002]/data[at0003]/items[at0095]/value = at0101"""
    }

    base_query = """SELECT e/ehr_id/value AS patient_identifier\nFROM Ehr e\n"""

    bp_st1 = build_structure(max_contains, 10, get_labels())
    bp_st2 = build_structure(max_contains, 10, get_labels())
    ua_st1 = build_structure(max_contains, 10, get_labels())
    ua_st2 = build_structure(max_contains, 10, get_labels())

    with open('matching_structure_file.json', 'w') as f:
        f.write(json.dumps([bp_st1, bp_st2, ua_st1, ua_st2]))

    for i in xrange(max_contains):
        cts['bp1'].append(_get_containment(i, bp_st1))
        cts['bp2'].append(_get_containment(i, bp_st2))
        cts['ua1'].append(_get_containment(i, ua_st1))
        cts['ua2'].append(_get_containment(i, ua_st2))

    mode = 'w'
    for k, v in cts.iteritems():
        for i in xrange(len(v)):
            query = base_query
            query += "%s\n%s\n" % ("\n".join(v[:i+1]), leaves[k[:2]])
            save_query(query, mode)
            mode = 'a'
            save_query("%s%s\n" % (query, wheres[k[:2]]), mode)





if __name__ == '__main__':

    parser = get_parser()
    args = parser.parse_args(sys.argv[1:])
    build_queries(args.max_contains)