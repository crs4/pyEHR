from random import randint, choice
from uuid import uuid4
from inspect import ismethod, isbuiltin, isfunction
import json

import archetype_builder
from pyehr.ehr.services.dbmanager.dbservices.wrappers import ArchetypeInstance
from archetype_builder import Composition


def build_record(record_description, archetypes_dir, forced_values=None):
    if isinstance(record_description, dict):
        for k, v in record_description.iteritems():
            if not k.startswith('composition'):
                raise ValueError('Container type "%s" unknown' % k)
            children = [build_record(x, archetypes_dir, forced_values) for x in v]
            composition_label = k.split('.')[1]
            return ArchetypeInstance(*archetype_builder.BUILDERS['composition'](archetypes_dir, children,
                                                                                composition_label).build())
    else:
        kw = {}
        if forced_values and record_description in forced_values:
            for k, v in forced_values[record_description].iteritems():
                if isfunction(v[0]) or ismethod(v[0]) or isbuiltin(v[0]):
                    kw[k] = (v[0])(*v[1:])
                else:
                    kw[k] = v[0]  # only take the first element of the tuple
        return ArchetypeInstance(*archetype_builder.BUILDERS[record_description](archetypes_dir, **kw).build())


def build_patient_dataset(dataset_conf, structures, archetypes_dir):
    """
    {
      "records_distribution": {
        2: 1,
        3: 0,
        "no_match": 49
      },
      "hits": {
        2: {
          "hits_count": 1,
          "hit_condition": {"blood_pressure": {"systolic": (randint, 10, 20)}},
          "no_hit_condition": {"blood_pressure": {"systolic": (randint, 200, 250)}}
        },
        3: {
          "hits_count": 0,
          "hit_condition": None,
          "no_hit_condition": {"blood_pressure": {"systolic": (randint, 200, 250)}}
        }
      }
    }
    """
    patient_label = 'PATIENT_%s' % uuid4().hex
    records = []
    # first of all, create non matching records
    no_match_count = dataset_conf['records_distribution'].pop('no_match')
    for _ in xrange(no_match_count):
        r = build_record(
            choice(structures['no_match']),
            archetypes_dir
        )
        records.append(r.to_json())
    # build records that will match queries
    for level in sorted(dataset_conf['records_distribution'], reverse=True):
        level_records_count = 0
        # build hits
        for _ in xrange(dataset_conf['hits'][level]['hits_count'] - level_records_count):
            r = build_record(
                choice(structures[level]),
                archetypes_dir,
                dataset_conf['hits'][level]['hit_condition']
            )
            records.append(r.to_json())
            level_records_count += 1
        # matching structures but no hits for where clause
        for _ in xrange(dataset_conf['records_distribution'][level] - level_records_count):
            r = build_record(
                choice(structures[level]),
                archetypes_dir,
                dataset_conf['hits'][level]['no_hit_condition']
            )
            records.append(r.to_json())
            level_records_count += 1
    return {patient_label: records}


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
            arch = ArchetypeInstance(*cls(archetypes_dir).build())
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