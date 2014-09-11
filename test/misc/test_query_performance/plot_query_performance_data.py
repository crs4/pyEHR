import sys, argparse, csv
from bokeh.plotting import *


def get_parser():
    parser = argparse.ArgumentParser('Plot data created with test_query_performance_batch.py')
    parser.add_argument('--input-file', type=str, required=True,
                        help='input CSV file')
    parser.add_argument('--output-file', type=str, required=True,
                        help='output file for bokeh')
    parser.add_argument('--exclude-creation-time', action='store_true',
                        help='Exclude creation time from chart')
    parser.add_argument('--exclude-cleanup-time', action='store_true',
                        help='Exclude cleanup time from chart')
    parser.add_argument('--log-file', type=str, help='LOG file (default stderr)')
    parser.add_argument('--log-level', type=str, default='INFO',
                        help='LOG level (default INFO)')
    return parser


def main(argv):
    parser = get_parser()
    args = parser.parse_args(argv)
    with open(args.input_file) as in_file:
        reader = csv.DictReader(in_file, delimiter='\t')
        x_axis_data = list()
        build_dataset_data = list()
        select_all_data = list()
        select_all_patient_data = list()
        filtered_query_data = list()
        filtered_patient_data = list()
        cleanup_data = list()
        for row in reader:
            x_axis_data.append(int(row['patients']) * int(row['ehrs_for_patient']))
            build_dataset_data.append(float(row['build_dataset_time']))
            select_all_data.append(float(row['select_all_time']))
            select_all_patient_data.append(float(row['select_all_patient_time']))
            filtered_query_data.append(float(row['filtered_query_time']))
            filtered_patient_data.append(float(row['filtered_patient_time']))
            cleanup_data.append(float(row['cleanup_time']))
    output_file(args.output_file)
    hold()
    if not args.exclude_creation_time:
        build_dataset_line = line(x_axis_data, build_dataset_data, color='black',
                                  line_dash='dashed')
        build_dataset_scatter = scatter(x_axis_data, build_dataset_data, color='black',
                                        legend='dataset creation time', size=4)
    select_all_line = line(x_axis_data, select_all_data, color='blue',
                           line_dash='dashed')
    select_all_scatter = scatter(x_axis_data, select_all_data, color='blue',
                                 size=4, legend='SELECT ALL query time')
    select_all_patient_line = line(x_axis_data, select_all_patient_data, color='red',
                                   line_dash='dashed')
    select_all_patient_scatter = scatter(x_axis_data, select_all_patient_data, color='red',
                                         size=4, legend='SELECT ALL for single patient query time')
    filtered_query_line = line(x_axis_data, filtered_query_data, color='green',
                               line_dash='dashed')
    filtered_query_scatter = scatter(x_axis_data, filtered_query_data, color='green',
                                     size=4, legend='FILTERED QUERY query time')
    filtered_patient_line = line(x_axis_data, filtered_patient_data, color='orange',
                                 line_dash='dashed')
    filtered_patient_scatter = scatter(x_axis_data, filtered_patient_data, color='orange',
                                       size=4, legend='FILTERED QUERY for single patient query time')
    if not args.exclude_cleanup_time:
        cleanup_line = line(x_axis_data, cleanup_data, color='yellow',
                            line_dash='dashed')
        cleanup_scatter = scatter(x_axis_data, cleanup_data, color='yellow',
                                  size=4, legend='dataset cleanup time')
    xaxis().axis_label = 'Dataset size'
    yaxis().axis_label = 'Execution time in seconds'
    curplot().title = 'pyEHR performance chart'
    legend().orientation = 'top_left'
    show()


if __name__ == '__main__':
    main(sys.argv[1:])