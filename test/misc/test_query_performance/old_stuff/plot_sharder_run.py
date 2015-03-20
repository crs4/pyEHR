import csv, numpy
from bokeh.plotting import *


def plot_scatter(measures, slot_count, column_label, color, legend, width=None, height=None):
    measures_x_axis = list()
    measures_y_axis = list()
    for m in measures:
        measures_x_axis.append(slot_count)
        measures_y_axis.append(float(m[column_label]))
    print measures_y_axis
    print measures_x_axis
    scatter(measures_x_axis, measures_y_axis, color=color, size=6, legend=legend,
            plot_width=width, plot_height=height, alpha=0.3)



def plot_mean_values(measures, slot_count, column_label, color, legend):
    mean_values = [numpy.mean([float(m[column_label]) for m in measures])]
    scatter(slot_count, mean_values, color=color, size=10, legend=legend)
    line(slot_count, mean_values, color=color, legend=legend, line_dash='dashed')

def main():
    with open('1M_run_global.tsv') as f:
        reader = csv.DictReader(f, delimiter='\t')
        rows = [r for r in reader]

    times = dict()
    for r in rows:
        times.setdefault(r['cluster_nodes'], []).append({
            'deep_1': r['deep_1'],
            'deep_1+w': r['deep_1+w'],
            'deep_2': r['deep_2'],
            'deep_2+w': r['deep_2+w'],
            'deep_3': r['deep_3'],
            'deep_3+w': r['deep_3+w'],
            'deep_4': r['deep_4'],
            'deep_4+w': r['deep_4+w']
        })

    w = None
    h = None

    output_file('/tmp/plot.html')
    hold()
    print times
    for i in xrange(3, 6):
        plot_scatter(times[str(i)], i, 'deep_1', 'navy', 'LEVEL 1 containment, no WHERE', w, h)
        plot_mean_values(times[str(i)], i, 'deep_1', 'navy', 'LEVEL 1 containment, no WHERE')
        plot_scatter(times[str(i)], i, 'deep_1+w', 'tomato', 'LEVEL 1 containment', w, h)
        plot_mean_values(times[str(i)], i, 'deep_1+w', 'navy', 'LEVEL 1 containment, no WHERE')
        plot_scatter(times[str(i)], i, 'deep_2', 'black', 'LEVEL 2 containment, no WHERE', w, h)
        plot_scatter(times[str(i)], i, 'deep_2+w', 'green', 'LEVEL 2 containment', w, h)
        plot_scatter(times[str(i)], i, 'deep_3', 'yellow', 'LEVEL 3 containment, no WHERE', w, h)
        plot_scatter(times[str(i)], i, 'deep_3+w', 'orange', 'LEVEL 3 containment', w, h)
        plot_scatter(times[str(i)], i, 'deep_4', 'blue', 'LEVEL 4 containment, no WHERE', w, h)
        plot_scatter(times[str(i)], i, 'deep_4+w', 'red', 'LEVEL 4 containment', w, h)
    xaxis().axis_label = 'Number of shards in the MongoDB cluster'
    yaxis().axis_label = 'Query time (in seconds)'
    curplot().title = 'pyEHR query performance chart'
    legend().orientation = 'top_right'
    show()

if __name__ == '__main__':
    main()