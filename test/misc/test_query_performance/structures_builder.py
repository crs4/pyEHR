import json
import numpy as np
import itertools as it
from random import randint

import archetype_builder

from pyehr.ehr.services.dbmanager.dbservices.wrappers import ArchetypeInstance
from archetype_builder import Composition


def get_random_subtree(max_depth, max_width):
    if max_depth == 0:
        leafs = [a for a in archetype_builder.BUILDERS.keys() if a != 'composition']
        children = [leafs[x] for x in np.random.randint(0, len(leafs), max_width)]
    else:
        # always go deeper with the first child node in order to achieve max_depth in at least one case
        children = [{'composition': get_random_subtree(max_depth-1, randint(1, max_width))}]
        for i in xrange(max_width-1):
            ch = archetype_builder.BUILDERS.keys()[randint(0, len(archetype_builder.BUILDERS)-1)]
            if ch == 'composition':
                ch = {'composition': get_random_subtree(max_depth-1, randint(1, max_width))}
            children.append(ch)
    return children


def build_structure(max_depth, max_width):
    return {'composition': get_random_subtree(max_depth-1, max_width)}


def build_structures(json_output_file, structures_count):
    structures = []
    for max_depth, max_width in it.izip([int(i) for i in np.random.normal(6, 1, structures_count)],
                                        [int(i) for i in np.random.uniform(1, 10, structures_count)]):
        if max_depth < 1:
            max_depth = 1
        elif max_depth > 11:
            max_depth = 11
        structures.append(build_structure(max_depth, max_width))
    with open(json_output_file, 'w') as f:
        f.write(json.dumps(structures))


def build_record(record_description, archetypes_dir):
    if isinstance(record_description, dict):
        for k, v in record_description.iteritems():
            if k != 'composition':
                raise ValueError('Container type %s unknown' % k)
            children = [build_record(x, archetypes_dir) for x in v]
            return ArchetypeInstance(*archetype_builder.BUILDERS[k](archetypes_dir, children).build())
    else:
        return ArchetypeInstance(*archetype_builder.BUILDERS[record_description](archetypes_dir).build())


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