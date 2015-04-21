import json, operator
import numpy as np
import itertools as it
from random import randint
from voluptuous import Schema, Required

import archetype_builder

from pyehr.utils import decode_dict


def get_composition_label(labels):
    return 'composition.%s' % labels[randint(0, len(labels) - 1)]


def _calculate_composition_label(forced_path, max_depth, nodes_blacklist, composition_labels):
    if 0 < len(forced_path) == max_depth+1:
        cmp_label = forced_path.pop(0)
        nodes_blacklist.append(cmp_label)
    else:
        cmp_label = get_composition_label(composition_labels)
        while cmp_label in nodes_blacklist:
            cmp_label = get_composition_label(composition_labels)
        if len(forced_path) > 0 and forced_path[0] == cmp_label:
            nodes_blacklist.append(forced_path.pop(0))
    return cmp_label, nodes_blacklist


def get_random_subtree(max_depth, max_width, composition_labels, forced_path=None, nodes_blacklist=None):
    if not nodes_blacklist:
        nodes_blacklist = []
    if not forced_path:
        forced_path = []
    if max_depth == 0:
        children = list()
        if len(forced_path) == 1:
            children.append(forced_path.pop(0))
            max_width -= 1
        leafs = [a for a in archetype_builder.BUILDERS.keys() if a != 'composition'
                 and a not in nodes_blacklist]
        for ch in children:
            # only one leaf must be of the type specified by the forced path
            leafs.remove(ch)
            nodes_blacklist.append(ch)
        children.extend([leafs[x] for x in np.random.randint(0, len(leafs), max_width)])
    else:
        # always go deeper with the first child node in order to achieve max_depth in at least one case
        cmp_label, nodes_blacklist = _calculate_composition_label(forced_path, max_depth,
                                                                  nodes_blacklist, composition_labels)
        st, bl = get_random_subtree(max_depth-1, randint(1, max_width),
                                    composition_labels, forced_path, nodes_blacklist)
        children = [{cmp_label: st}]
        nodes_blacklist.extend(bl)
        for i in xrange(max_width-1):
            nodes = [a for a in archetype_builder.BUILDERS.keys() if a != 'composition'
                     and a not in nodes_blacklist]
            # the probability for the next node to be a leaf (not a composition) in related to the
            # max_depth achieved by the algorithm
            leaf_threshold = int(round(1./(max_depth+1) * 100))
            # randomly extract a number between 1 and 100
            toss = randint(1, 100)
            if toss > 0 and leaf_threshold <= toss:
                ch = nodes[randint(0, len(nodes)-1)]
                if len(forced_path) == 1 and ch == forced_path[0]:
                    forced_path.remove(ch)
                    nodes_blacklist.append(ch)
            else:
                ch = 'composition'
            if ch.startswith('composition'):
                cmp_label, nodes_blacklist = _calculate_composition_label(forced_path, max_depth,
                                                                          nodes_blacklist, composition_labels)
                st, bl = get_random_subtree(max_depth-1, randint(1, max_width), composition_labels,
                                            forced_path, nodes_blacklist)
                ch = {cmp_label: st}
                nodes_blacklist.extend(bl)
            children.append(ch)
    nodes_blacklist = list(set(nodes_blacklist))
    return children, nodes_blacklist


def build_structure(max_depth, max_width, labels, forced_path=None, ignored_compositions=None):
    if not forced_path:
        forced_path = []
    if not ignored_compositions:
        ignored_compositions = []
    if len(forced_path) > max_depth:
        # forced_path can't be applied
        raise ValueError('Unable to apply forced path %r for structure with maximum depth of %d' %
                         (forced_path, max_depth))
    if len(forced_path) == max_depth:
        cmp_label = forced_path[0]
    else:
        cmp_label = get_composition_label(labels)
        while cmp_label in ignored_compositions:
            cmp_label = get_composition_label(labels)
    if 0 < len(forced_path) <= max_depth and cmp_label == forced_path[0]:
        ignored_compositions.append(cmp_label)
        forced_path.pop(0)
    st, _ = get_random_subtree(max_depth-1, max_width, labels, forced_path, ignored_compositions)
    return {cmp_label: st}


def check_builder_configuration(builder_conf):
    builder_conf_schema = Schema({
        Required('compositions_count'): int,
        Required('compositions_lbl_start_index'): int,
        Required('mean_depth'): int,
        Required('max_width'): int,
        Required('structures_count'): int,
        Required('full_query_path'): list,
        Required('matching_structures'): dict
    })
    builder_conf_schema(builder_conf)
    low_level = builder_conf['matching_structures'][sorted(builder_conf['matching_structures'])[0]]
    for level in sorted(builder_conf['matching_structures'])[1:]:
        current_level = builder_conf['matching_structures'][level]
        assert current_level <= low_level
        low_level = current_level
        assert 0 < level <= len(builder_conf['full_query_path'])


def get_forced_path(level, full_path):
    fpath_index = range(0, level-1)
    fpath_index.append(-1)
    forced_path = list(operator.itemgetter(*fpath_index)(full_path))
    ignored_path = [x for x in full_path if x not in forced_path]
    return forced_path, ignored_path


def get_labels(labels_set_size=20, labels_start_index=0):
    return ['lbl-%05d' % x for x in xrange(labels_start_index,
                                           labels_start_index+labels_set_size)]


def normalize_keys(dict_to_normalize):
    normalized_dict = dict()
    for k, v in dict_to_normalize.iteritems():
        try:
            normalized_dict[int(k)] = v
        except ValueError:
            normalized_dict[k] = v
    return normalized_dict


def build_structures(builder_conf):
    """
    builder_conf field is a dictionary used to describe how the run is configured.
    The dictionary should be like

    {
      "compositions_count": 20,
      "compositions_lbl_start_index": 0,
      "mean_depth": 4,
      "max_width": 5,
      "structures_count": 100,
      "full_query_path": ["composition.foo", "composition.bar", "liver_function"],
      "matching_structures": {
        2: 50,
        3: 25
      }
    }

    where
    * "full_query_path" is the full ordered path matching the CONTAINS clause of the
      deeper query that will be executed using the produced structures
    * "matching_structures" is a dictionary with depth levels as keys and the percentage
      of matching structures for the given level that will be created calculated over the
      "structures_count" value
    The total number of the matching structures is obtained as the x% of the "structures_count"
    field, where "x" is the value of the lower level of the "matching_structures" field.
    For each key in the "matching_structures" field, values must follow the rule

                                values[key] <= values[key-1]

    because all the structures that match the query for the level "key" automatically will
    match query for level "key-1".
    """
    builder_conf['matching_structures'] = normalize_keys(builder_conf['matching_structures'])
    check_builder_configuration(builder_conf)
    structures = {}
    created_matching_str = 0
    labels = get_labels(builder_conf['compositions_count'], builder_conf['compositions_lbl_start_index'])
    for level in sorted(builder_conf['matching_structures'], reverse=True):
        structures[level] = []
        str_count = round((builder_conf['structures_count']/100.) *
                          builder_conf['matching_structures'][level]) - created_matching_str
        min_depth = level
        for depth, width in it.izip([int(i) for i in np.random.normal(builder_conf['mean_depth'],
                                                                      min_depth,
                                                                      str_count)],
                                    [int(i) for i in np.random.uniform(1, builder_conf['max_width'],
                                                                       str_count)]):
            fpath, ignore_path = get_forced_path(level, builder_conf['full_query_path'])
            if depth < min_depth:
                depth = min_depth
            elif depth > builder_conf['mean_depth'] + (builder_conf['mean_depth']-1):
                depth = builder_conf['mean_depth'] + (builder_conf['mean_depth']-1)
            structures[level].append(build_structure(depth, width, labels, forced_path=fpath,
                                                     ignored_compositions=ignore_path))
        created_matching_str += str_count
    non_matching_structures = builder_conf['structures_count'] - created_matching_str
    for depth, width in it.izip([int(i) for i in np.random.normal(builder_conf['mean_depth'], 1,
                                                                  non_matching_structures)],
                                [int(i) for i in np.random.uniform(1, builder_conf['max_width'],
                                                                   non_matching_structures)]):
        if depth < 1:
            depth = 1
        elif depth > builder_conf['mean_depth'] + (builder_conf['mean_depth'] - 1):
            depth = builder_conf['mean_depth'] + builder_conf['mean_depth'] - 1
        structures.setdefault('no_match', []).append(
            build_structure(depth, width, labels,
                            ignored_compositions=builder_conf['full_query_path'][:-1])
        )
    return structures


def structures_to_json(structures, json_output_file):
    with open(json_output_file, 'w') as f:
        f.write(json.dumps(structures))


def get_structures_from_file(structures_file):
    with open(structures_file) as f:
        return normalize_keys(decode_dict(json.loads(f.read())))
