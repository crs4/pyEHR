import sys, argparse, csv, os

from test_query_performance import QueryPerformanceTest
from structures_builder import build_structures


def get_parser():
    parser = argparse.ArgumentParser('Run a batch of query performance tests')
    parser.add_argument('--conf-file', type=str, required=True,
                        help='pyEhr configuration file')
    parser.add_argument('--batch-description-file', type=str, required=True,
                        help='The CSV file with the description of the batchs')
    parser.add_argument('--structures-description-dir', type=str, default='/tmp',
                        help='The directory where EHRs description files are going to be stored')
    parser.add_argument('--output-files-dir', type=str, required=True,
                        help='The directory where output files will be stored')
    parser.add_argument('--output-file-basename', type=str, required=True,
                        help='The basename of the file that will contain the results')
    parser.add_argument('--archetype-dir', type=str, required=True,
                        help='The directory containing archetype in json format')
    parser.add_argument('--run-cycles', type=int, default=1,
                        help='The number of cycles the run will be repeated')
    parser.add_argument('--build-dataset-threads', type=int, default=1,
                        help='The number of threads that will be used to create the dataset (default 1)')
    parser.add_argument('--log-file', type=str, help='LOG file (default stderr)')
    parser.add_argument('--log-level', type=str, default='INFO',
                        help='LOG level (default INFO)')
    return parser


def get_test_description(batch_file):
    with open(batch_file) as f:
        reader = csv.DictReader(f, delimiter='\t')
        batches = [row for row in reader]
    batch_descriptions = dict()
    for b in batches:
        batch_descriptions.setdefault((int(b['mean_depth']), int(b['max_width']), int(b['structures'])),
            []).append((int(b['patients']), int(b['ehrs_for_patient']), int(b['matching_instances'])))
    return batch_descriptions


def get_output_writer(out_file, out_dir):
    ofile = open(os.path.join(out_dir, out_file), 'w')
    writer = csv.DictWriter(ofile, ['patients', 'ehrs_for_patient',
                                    'select_all_time', 'select_all_patient_time',
                                    'filtered_query_time', 'filtered_patient_time',
                                    'patient_count_time'], delimiter='\t')
    writer.writeheader()
    return writer, ofile


def run_test(patients_size, ehr_size, conf_file, archetypes_dir, ehr_structures_file, matching_instances,
             logfile, loglevel, build_dataset_threads=1, clean_dataset=False, db_name_prefix=None):
    qpt = QueryPerformanceTest(conf_file, archetypes_dir, ehr_structures_file, matching_instances,
                                logfile, loglevel, db_name_prefix)
    select_all_time, select_all_patient_time, filtered_query_time,\
    filtered_patient_time, patient_count_time = qpt.run(patients_size, ehr_size,
                                                        clean_dataset, build_dataset_threads)
    return {
        'select_all_time': select_all_time,
        'select_all_patient_time': select_all_patient_time,
        'filtered_query_time': filtered_query_time,
        'filtered_patient_time': filtered_patient_time,
        'patient_count_time': patient_count_time,
    }


def main(argv):
    parser = get_parser()
    args = parser.parse_args(argv)
    for cycle in xrange(0, args.run_cycles):
        cycle_description = get_test_description(args.batch_description_file)
        for str_def, batches in cycle_description.iteritems():
            depth = str_def[0]
            width = str_def[1]
            str_count = str_def[2]
            str_def_label = 'd-%d_w-%d_str-%d' % (depth, width, str_count)
            str_out_file = os.path.join(args.structures_description_dir,
                                        '%s_cycle-%d.json' % (str_def_label, cycle))
            build_structures(str_out_file, str_count, depth, width)
            out_file_writer, out_file = get_output_writer('%s_%s_cycle-%d.tsv' %
                                                          (args.output_file_basename,
                                                           str_def_label, cycle),
                                                          args.output_files_dir)
            batch = sorted(batches)[0]
            results = run_test(batch[0], batch[1], args.conf_file,
                               args.archetype_dir, str_out_file, batch[2],
                               args.log_file, args.log_level,
                               build_dataset_threads=args.build_dataset_threads,
                               clean_dataset=True, db_name_prefix=str_def_label)
            results.update({'patients': batch[0], 'ehrs_for_patient': batch[1]})
            out_file_writer.writerow(results)
            for batch in sorted(batches)[1:]:
                results = run_test(batch[0], batch[1], args.conf_file,
                                   args.archetype_dir, str_out_file, batch[2],
                                   args.log_file, args.log_level,
                                   build_dataset_threads=args.build_dataset_threads,
                                   db_name_prefix=str_def_label)
                results.update({'patients': batch[0], 'ehrs_for_patient': batch[1]})
                out_file_writer.writerow(results)
            out_file.close()


if __name__ == '__main__':
    main(sys.argv[1:])