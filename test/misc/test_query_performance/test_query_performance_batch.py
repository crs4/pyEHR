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
    parser.add_argument('--output-file-basename', type=str, required=True,
                        help='The basename of the file that will contain the results')
    parser.add_argument('--archetype-dir', type=str, required=True,
                        help='The directory containing archetype in json format')
    parser.add_argument('--run-cycles', type=int, default=1,
                        help='The number of cycles the run will be repeated')
    parser.add_argument('--log-file', type=str, help='LOG file (default stderr)')
    parser.add_argument('--log-level', type=str, default='INFO',
                        help='LOG level (default INFO)')
    return parser


def get_test_description(batch_file):
    with open(batch_file) as f:
        reader = csv.DictReader(f, delimiter='\t')
        batches = [row for row in reader]
    return batches


def get_output_writer(out_file):
    ofile = open(out_file, 'w')
    writer = csv.DictWriter(ofile, ['patients', 'ehrs_for_patient',
                                    'select_all_time', 'select_all_patient_time',
                                    'filtered_query_time', 'filtered_patient_time',
                                    'patient_count_time'], delimiter='\t')
    writer.writeheader()
    return writer, ofile


def run_test(patients_size, ehr_size, conf_file, archetypes_dir, ehr_structures_file,
             logfile, loglevel):
    qpt = QueryPerformanceTest(conf_file, archetypes_dir, ehr_structures_file,
                               logfile, loglevel)
    select_all_time, select_all_patient_time, filtered_query_time,\
    filtered_patient_time, patient_count_time = qpt.run(patients_size, ehr_size)
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
        str_out_file = os.path.join(args.structures_description_dir,
                                    'structures_cycle_%d.json' % cycle)
        build_structures(str_out_file, 50)
        batches = get_test_description(args.batch_description_file)
        out_file_writer, out_file = get_output_writer('%s_%d.tsv' %
                                                      (args.output_file_basename, cycle))
        for batch in batches:
            results = run_test(int(batch['patients']), int(batch['ehrs_for_patient']),
                               args.conf_file, args.archetype_dir, str_out_file,
                               args.log_file, args.log_level)
            results.update(batch)
            out_file_writer.writerow(results)
        out_file.close()


if __name__ == '__main__':
    main(sys.argv[1:])