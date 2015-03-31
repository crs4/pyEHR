from random import choice
import os, gzip

try:
    import simplejson as json
except ImportError:
    import json

import archetype_builder
from value_setters import set_value
from structures_builder import normalize_keys
from pyehr.utils import decode_dict
from pyehr.ehr.services.dbmanager.dbservices.wrappers import ArchetypeInstance, ClinicalRecord, PatientRecord


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
                kw[k] = set_value(*v)
        return ArchetypeInstance(*archetype_builder.BUILDERS[record_description](archetypes_dir, **kw).build())


def build_patient_dataset(patient_label, dataset_conf, structures, archetypes_dir):
    """
    {
      "records_count": 50
      "records_distribution": {
        2: 1,
        3: 0,
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
    records = []
    # build records that will match queries
    for level in sorted(dataset_conf['records_distribution'], reverse=True):
        level_records_count = 0
        # build hits
        if level in dataset_conf['hits'] and 'hits_count' in dataset_conf['hits'][level]:
            for _ in xrange(dataset_conf['hits'][level]['hits_count'] - level_records_count):
                r = build_record(
                    choice(structures[level]),
                    archetypes_dir,
                    dataset_conf['hits'][level]['hit_condition']
                )
                records.append((level, r.to_json()))
                level_records_count += 1
        # matching structures but no hits for where clause
        if level in dataset_conf['records_distribution']:
            for _ in xrange(dataset_conf['records_distribution'][level] - level_records_count):
                r = build_record(
                    choice(structures[level]),
                    archetypes_dir,
                    dataset_conf['hits'][level]['no_hit_condition']
                )
                records.append((level, r.to_json()))
                level_records_count += 1
    for _ in xrange(dataset_conf['records_count'] - len(records)):
        r = build_record(
            choice(structures['no_match']),
            archetypes_dir
        )
        records.append(('no_match', r.to_json()))
    return {patient_label: records}


def resolve_percentage(percentage, total):
    return round((percentage * total) / 100.)


def build_dataset(dataset_conf):
    """
    {
      "records_count": 1000,
      "patients_count": 10,
      "hit_condition": {"blood_pressure": {"systolic": (randint, 10, 20)}},
      "no_hit_condition": {"blood_pressure": {"systolic": (randint, 200, 250)}}
      "records_distribution": {
        2: {
          "matches": 10, // percentage on records_count
          "hits": 5,     // percentage on records_count
        }
      }
    }
    """
    patients_map = {}
    # get records count
    for x in xrange(dataset_conf['patients_count']):
        plabel = 'PATIENT_%d' % x
        patients_map[plabel] = {
            'records_count': int(round(dataset_conf['records_count'] / float(dataset_conf['patients_count']))),
            'records_distribution': {},
            'hits': {}
        }
    if dataset_conf['records_count'] % dataset_conf['patients_count'] != 0:
        for x in xrange(dataset_conf['records_count'] % dataset_conf['patients_count']):
            patients_map[patients_map.keys()[x]]['records_count'] += 1
    # get hits distribution
    previous_level_hits = 0
    previous_level_matches = 0
    dataset_conf['records_distribution'] = normalize_keys(dataset_conf['records_distribution'])
    for level, conf in sorted(dataset_conf['records_distribution'].iteritems(), reverse=True):
        hits_count = (resolve_percentage(conf['hits'], dataset_conf['records_count'])) - previous_level_hits
        if hits_count < 0:
            hits_count = 0
        for x in xrange(int(hits_count)):
            hmap = patients_map[choice(patients_map.keys())]
            hmap['hits'].setdefault(level, {})
            hmap['hits'][level].setdefault('hits_count', 0)
            hmap['records_distribution'].setdefault(level, 0)
            hmap['hits'][level]['hits_count'] += 1
            hmap['records_distribution'][level] += 1
            hmap['hits'][level].setdefault('hit_condition', dataset_conf['hit_condition'])
            hmap['hits'][level].setdefault('no_hit_condition', dataset_conf['no_hit_condition'])
        previous_level_hits += hits_count
        # get matches  distribution
        matches_count = (resolve_percentage(conf['matches'], dataset_conf['records_count'])) -\
            previous_level_matches - previous_level_hits
        if matches_count < 0:
            matches_count = 0
        for x in xrange(int(matches_count)):
            patient_key = choice(patients_map.keys())
            pmap = patients_map[patient_key]['records_distribution']
            patients_map[patient_key].setdefault('hits', {})
            pmap.setdefault(level, 0)
            pmap[level] += 1
            # set NO MATCH condition for current level
            if level not in patients_map[patient_key]['hits']:
                patients_map[patient_key]['hits'].setdefault(
                    level, {'no_hit_condition': dataset_conf['no_hit_condition']}
                )
        previous_level_matches += matches_count
    return patients_map


def get_patient_records(patient_datasets, structures, archetype_dir):
    for patient, dataset_conf in patient_datasets.iteritems():
        yield build_patient_dataset(patient, dataset_conf, structures, archetype_dir)


def get_patient_records_from_file(patient_datasets_file, compressed_file=False):
    if compressed_file:
        f = gzip.open(patient_datasets_file, 'rb')
    else:
        f = open(patient_datasets_file)
    for row in f.readlines():
        for prec, crecs in get_patient_records_from_file_row(row):
            yield prec, crecs


def get_patient_records_from_file_row(file_row):
    patient_dataset = normalize_keys(decode_dict(json.loads(file_row)))
    for patient_id, conf in patient_dataset.iteritems():
        clinical_records = [ClinicalRecord(ArchetypeInstance(**c[1])) for c in conf]
        patient_record = PatientRecord(patient_id)
        yield patient_record, clinical_records


def patient_records_to_json(patient_datasets, structures, archetype_dir, json_output_file,
                            compression_enabled=False):
    if compression_enabled:
        f = gzip.open(json_output_file, 'wb')
    else:
        f = open(json_output_file, 'w')
    for patient_records in get_patient_records(patient_datasets, structures, archetype_dir):
        f.write(json.dumps(patient_records) + os.linesep)
    f.close()
