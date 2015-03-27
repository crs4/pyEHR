import os, argparse, sys, time, json

from structures_builder import build_structures, structures_to_json, get_structures_from_file
from records_builder import build_dataset, patient_records_to_json

from pyehr.utils import get_logger, decode_dict


def get_parser():
    parser = argparse.ArgumentParser('Build structures and patients datasets JSON files')
    parser.add_argument('--structures_config_file', type=str, required=True,
                        help='The JSON file with structures\' definitions')
    parser.add_argument('--datasets_config_file', type=str, required=True,
                        help='The JSON file with datasets\' definitions')
    parser.add_argument('--archetypes_dir', type=str, required=True,
                        help='The directory that contains Archetypes\' definition in JSON format')
    parser.add_argument('--structures_out_file', type=str, required=True,
                        help='The JSON file used to store generated structures, ' +
                             'if file already exists no structures will be generated')
    parser.add_argument('--overwrite_structures_file', action='store_true',
                        help='If structures file already exists, overwrite it')
    parser.add_argument('--datasets_out_file', type=str, required=True,
                        help='The JSON file used to store generated patients datasets ' +
                        '(if compression in enabled, the .gz extension will be automatically added)')
    parser.add_argument('--compression_enabled', action='store_true',
                        help='Enable compression for patients datasets file ' +
                             '(file will be compressed using Python\'s gzip lib)')
    parser.add_argument('--log_file', type=str, help='LOG file (default=stderr)')
    parser.add_argument('--log_level', type=str, default='INFO',
                        help='LOG level (default=INFO)')
    return parser


def create_structures(structs_conf_file, structs_out_file, overwrite, logger):
    with open(structs_conf_file) as f:
        structs_conf = decode_dict(json.loads(f.read()))
    if os.path.isfile(structs_out_file) and not overwrite:
        logger.info('No need to build new structures file, loading existing ones from %s' % structs_out_file)
        structures = get_structures_from_file(structs_out_file)
    else:
        logger.info('Start building structures')
        start_time = time.time()
        structures = build_structures(structs_conf)
        logger.info('Structures building completed in %f seconds' % (time.time() - start_time))
        logger.info('Serializing structures into %s' % structs_out_file)
        structures_to_json(structures, structs_out_file)
    return structures


def create_datasets(dataset_conf_file, archetypes_dir, structs, dataset_out_file,
                    compressed_output, logger):
    with open(dataset_conf_file) as f:
        dataset_conf = decode_dict(json.loads(f.read()))
    logger.info('Start building dataset')
    start_time = time.time()
    dataset = build_dataset(dataset_conf)
    logger.info('Dataset building completed in %f seconds' % (time.time() - start_time))
    if compressed_output:
        dataset_out_file = '%s.gz' % dataset_out_file
    logger.info('Serializing patients records to %s' % dataset_out_file)
    start_time = time.time()
    patient_records_to_json(dataset, structs, archetypes_dir,
                            dataset_out_file, compressed_output)
    logger.info('Records serialized in %f seconds' % (time.time() - start_time))


def main(argv):
    parser = get_parser()
    args = parser.parse_args(argv)
    logger = get_logger('datasets_builder', log_level=args.log_level, log_file=args.log_file)

    structures = create_structures(args.structures_config_file, args.structures_out_file,
                                   args.overwrite_structures_file, logger)
    create_datasets(args.datasets_config_file, args.archetypes_dir, structures,
                    args.datasets_out_file, args.compression_enabled, logger)

if __name__ == '__main__':
    main(sys.argv[1:])