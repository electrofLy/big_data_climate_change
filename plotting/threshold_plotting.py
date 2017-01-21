import ast
import matplotlib.pyplot as plt
import numpy


def plot_threshold(data):
    """
        Plots a graph.
        :param data: Data is a list of dictionaries with keys id and count.
        :return: None
        """
    ids = []
    count = []
    for data_obj in data:
        ids.append(data_obj['id'])
        count.append(data_obj['count'])
    line, = plt.plot(ids, count, label='Threshold')
    plt.ticklabel_format(useOffset=False)
    plt.legend(handles=[line], loc=2)
    plt.xlabel('Number of matches')
    plt.ylabel('Count of matches')
    stats = 'Minimum: ' + str(numpy.min(count)) \
            + '\n' + 'Maximum: ' + str(int(numpy.max(count))) \
            + '\n' + 'Average: ' + str(int(numpy.average(count)))
    plt.annotate(stats, xy=(1, 1), xycoords='axes fraction', fontsize=16, xytext=(-5, -5), textcoords='offset points',
                 ha='right', va='top')
    # Logarithmic scale
    plt.yscale('log')
    plt.show()


# Read file and assign
fname = './threshold_volkskrant_output/result.dat'
with open(fname) as f:
    content = f.readlines()

# Raw data
threshold_raw = content
# List of objects
threshold_list = []
for obj in threshold_raw:
    # Parse objects to tuples
    threshold_list.append(ast.literal_eval(obj))

idx = 0
for obj in threshold_list:
    # Parse tuples to dictionaries
    new_dict = {'id': int(obj[0]), 'count': obj[1]}
    threshold_list[idx] = new_dict
    idx += 1

# Sort by id
sorted_thresholds = sorted(threshold_list, key=lambda k: k['id'])
plot_threshold(sorted_thresholds)
