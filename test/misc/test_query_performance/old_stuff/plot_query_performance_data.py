import sys, argparse, csv, os, numpy
from bokeh.plotting import *


def get_parser():
    parser = argparse.ArgumentParser('Plot data created with test_query_performance_batch.py')
    parser.add_argument('--input-files-dir', type=str, required=True,
                        help='the directory containing the input CSV files')
    parser.add_argument('--input-files-basename', type=str, required=True,
                        help='basename that will be used to search input files containing data to be plotted')
    parser.add_argument('--output-file', type=str, required=True,
                        help='output file for bokeh')
    parser.add_argument('--plot-height', type=int, help='Height of the plot (in pixels)')
    parser.add_argument('--plot-width', type=int, help='Width of the plot (in pixels)')
    parser.add_argument('--log-file', type=str, help='LOG file (default stderr)')
    parser.add_argument('--log-level', type=str, default='INFO',
                        help='LOG level (default INFO)')
    return parser


def get_files(files_dir, files_basename):
    return [os.path.join(files_dir, f) for f in os.listdir(files_dir)
            if f.startswith(files_basename)]


def extract_data(files):
    data = dict()
    for f in files:
        print 'opening %s' % f
        with open(f) as in_file:
            reader = csv.DictReader(in_file, delimiter='\t')
            for row in reader:
                times = {
                    'bp_patient_time': float(row['bp_patient_time']),
                    'ua_patient_time': float(row['ua_patient_time']),
                    'intersection_time': float(row['intersection_time']),
                }
                data.setdefault(int(row['patients'])*int(row['ehrs_for_patient']), []).append(times)
    return data


def plot_column(data, column_label, color, legend, width=None, height=None):
    """
    Plot values of a single column, mean value and dashed line
    """
    measures_x_axis = list()
    measures_y_axis = list()
    mean_values = list()
    for dataset_size, measures in sorted(data.iteritems()):
        print 'Calculating values for data_size %d' % dataset_size
        for measure in measures:
            measures_x_axis.append(dataset_size)
            measures_y_axis.append(measure[column_label])
        mean_values.append(numpy.mean([m[column_label] for m in measures]))
    print mean_values
    scatter(measures_x_axis, measures_y_axis, color=color, alpha=0.3, size=6, legend=legend,
            plot_width=width, plot_height=height)
    scatter(sorted(data.keys()), mean_values, color=color, alpha=1, size=8, legend=legend)
    line(sorted(data.keys()), mean_values, color=color, legend=legend, line_dash='dashed')


def main(argv):
    parser = get_parser()
    args = parser.parse_args(argv)
    files = get_files(args.input_files_dir, args.input_files_basename)
    data = extract_data(files)
    # start plotting
    output_file(args.output_file)
    hold()
    plot_column(data, 'bp_patient_time', 'navy', 'Query 1 (filtered patient ID retrieval)',
                args.plot_width, args.plot_height)
    plot_column(data, 'ua_patient_time', 'red', 'Query 2 (filtered patient ID retrieval)',
                args.plot_width, args.plot_height)
    plot_column(data, 'intersection_time', 'green', 'Query 1 and Query 2 intersection',
                args.plot_width, args.plot_height)
    xaxis().axis_label = 'Clinical Records count'
    yaxis().axis_label = 'Query execution time (in seconds)'
    curplot().title = 'pyEHR query performance chart'
    legend().orientation = 'top_left'
    show()


if __name__ == '__main__':
    main(sys.argv[1:])