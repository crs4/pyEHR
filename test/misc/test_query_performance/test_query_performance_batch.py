import sys, argparse, csv

from test_query_performance import QueryPerformanceTest


def get_parser():
    parser = argparse.ArgumentParser('Run a batch of query performance tests')
    parser.add_argument('--conf-file', type=str, required=True,
                        help='pyEhr configuration file')
    parser.add_argument('--batch-description-file', type=str, required=True,
                        help='The CSV file with the description of the batchs')
    parser.add_argument('--output-file', type=str, required=True,
                        help='The file that will contain the results')
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
    writer = csv.DictWriter(ofile, ['patients', 'ehrs_for_patient', 'build_dataset_time',
                                    'select_all_time', 'select_all_patient_time',
                                    'filtered_query_time', 'filtered_patient_time',
                                    'cleanup_time'], delimiter='\t')
    writer.writeheader()
    return writer, ofile


def run_test(patients_size, ehr_size, conf_file, logfile, loglevel):
    qpt = QueryPerformanceTest(conf_file, logfile, loglevel)
    build_dataset_time, select_all_time, select_all_patient_time, filtered_query_time,\
    filtered_patient_time, cleanup_time = qpt.run(patients_size, ehr_size)
    return {
        'build_dataset_time': build_dataset_time,
        'select_all_time': select_all_time,
        'select_all_patient_time': select_all_patient_time,
        'filtered_query_time': filtered_query_time,
        'filtered_patient_time': filtered_patient_time,
        'cleanup_time': cleanup_time
    }


def main(argv):
    parser = get_parser()
    args = parser.parse_args(argv)
    batches = get_test_description(args.batch_description_file)
    out_file_writer, out_file = get_output_writer(args.output_file)
    for batch in batches:
        results = run_test(int(batch['patients']), int(batch['ehrs_for_patient']),
                           args.conf_file, args.log_file, args.log_level)
        results.update(batch)
        out_file_writer.writerow(results)
    out_file.close()


if __name__ == '__main__':
    main(sys.argv[1:])