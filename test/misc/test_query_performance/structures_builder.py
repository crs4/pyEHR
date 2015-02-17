import json, operator
import numpy as np
import itertools as it
from random import randint
from voluptuous import Schema, Required

import archetype_builder

from pyehr.ehr.services.dbmanager.dbservices.wrappers import ArchetypeInstance
from archetype_builder import Composition


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
        forced_path = []
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
    return list(operator.itemgetter(*fpath_index)(full_path))


def build_structures(json_output_file, builder_conf):
    """
    build_conf field is a dictionary used to describe how the run is configured.
    The dictionary should be like

    {
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
    check_builder_configuration(builder_conf)
    structures = {}
    created_matching_str = 0
    labels = get_labels()
    for level in sorted(builder_conf['matching_structures'], reverse=True):
        structures[level] = []
        fpath = get_forced_path(level, builder_conf['full_query_path'])
        str_count = round((builder_conf['structures_count']/100.) *
                          builder_conf['matching_structures'][level]) - created_matching_str
        for depth, width in it.izip([int(i) for i in np.random.normal(builder_conf['mean_depth'], 1,
                                                                      str_count)],
                                    [int(i) for i in np.random.uniform(1, builder_conf['max_width'],
                                                                       str_count)]):
            if depth < 1:
                depth = 1
            elif depth > builder_conf['mean_depth'] + (builder_conf['mean_depth']-1):
                depth = builder_conf['mean_depth'] + (builder_conf['mean_depth']-1)
            structures[level].append(build_structure(depth, width, labels,
                                                     forced_path=fpath))
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
    with open(json_output_file, 'w') as f:
        f.write(json.dumps(structures))


def build_record(record_description, archetypes_dir, match, record_to_match=None):
    if isinstance(record_description, dict):
        for k, v in record_description.iteritems():
            if not k.startswith('composition'):
                raise ValueError('Container type %s unknown' % k)
            children = [build_record(x, archetypes_dir, match, record_to_match) for x in v]
            composition_label = k.split('.')[1]
            return ArchetypeInstance(*archetype_builder.BUILDERS['composition'](archetypes_dir, children,
                                                                                composition_label).build())
    else:
        kw = {}
        if record_description == 'blood_pressure':
            if match and record_to_match == 'blood_pressure':
                kw.update({'systolic': randint(121, 130), 'dyastolic': randint(80, 90)})
            else:
                kw.update({'systolic': randint(100, 105), 'dyastolic': randint(60, 75)})
        elif record_description == 'urin_analysis':
            if match and record_to_match == 'urin_analysis':
                kw.update({'glucose': 'at0120', 'protein': 'at0101'})
            else:
                glucose_values = ['at0115', 'at0116', 'at0117', 'at0118', 'at0119']
                protein_values = ['at0096', 'at0097', 'at0098', 'at0099', 'at0100']
                kw.update({
                    'glucose': glucose_values[randint(0, len(glucose_values)-1)],
                    'protein': protein_values[randint(0, len(protein_values)-1)]
                })
        return ArchetypeInstance(*archetype_builder.BUILDERS[record_description](archetypes_dir, **kw).build())


def contains_archetype(structure_description, archetype_label):
    element_label = archetype_label[0]
    if isinstance(structure_description, dict):
        for k, v in structure_description.iteritems():
            if not k.startswith('composition'):
                raise ValueError('Container type %s unknown' % k)
            if k == element_label:
                to_be_checked = archetype_label[1:]
            else:
                to_be_checked = archetype_label
            for child in v:
                matched, leaf = contains_archetype(child, to_be_checked)
                if matched:
                    return True, leaf
    else:
        if structure_description == element_label:
            return True, element_label
    return False, None


def get_labels(labels_set_size=20):
    return ['lbl-%05d' % x for x in xrange(0, labels_set_size)]


def _build_record_full_random(max_width, height, archetypes_dir):
    def _get_random_builder(_builders):
        builder_idx = randint(0, len(_builders)-1)
        cls = archetype_builder.get_builder( _builders[builder_idx] )
        return cls

    if height < 1:
        raise ValueError('Height must be greater than 0')

    builders = archetype_builder.BUILDERS.keys()

    if height == 1: # if height is zero it creates a leaf archetype (i.e an Observation)
        # deletes the composition from the possible builders
        leaf_builders = [b for b in builders if b != 'composition']
        children = []
        for i in xrange(max_width):
            cls = _get_random_builder(leaf_builders)
            arch = ArchetypeInstance( *cls(archetypes_dir).build() )
            children.append(arch)

    else:
        width = randint(1, max_width)
        arch = _build_record_full_random(width, height - 1, archetypes_dir)
        children = [arch]

        # creates the other children. They can be Composition or Observation
        for i in xrange(max_width - 1):
            cls = _get_random_builder(builders)
            if cls == Composition:
                width = randint(1, max_width)
                arch = _build_record_full_random(width, height - 1, archetypes_dir)
            else:
                arch = ArchetypeInstance( *cls(archetypes_dir).build() )
            children.append(arch)

    return ArchetypeInstance(*Composition(archetypes_dir, children).build())