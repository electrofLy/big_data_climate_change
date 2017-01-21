import ast
import matplotlib.pyplot as plt
import numpy


def plot_threshold(data):
    """
    Plots a histogram.
    :param data: Data is a list of dictionaries with keys id and count.
    :return: None
    """
    ids = []
    count = []
    for data_obj in data:
        ids.append(data_obj['id'])
        count.append(data_obj['count'])
    plt.hist(ids)
    plt.hist(count)
    # line, = plt.plot(bins, patches,'r--', label='Threshold')
    plt.ticklabel_format(useOffset=False)
    # plt.legend(handles=[line], loc=2)
    plt.xlabel('Id')
    plt.ylabel('Count')
    stats = 'Minimum: ' + str(numpy.min(count)) \
            + '\n' + 'Maximum: ' + str(int(numpy.max(count))) \
            + '\n' + 'Average: ' + str(int(numpy.average(count)))
    plt.annotate(stats, xy=(1, 1), xycoords='axes fraction', fontsize=16, xytext=(-5, -5), textcoords='offset points',
                 ha='right', va='top')
    plt.show()


# Read file and assign
fname = './histogram_char_count/histogramCharCount.dat'
with open(fname) as f:
    content = f.readlines()

# Raw data
histogram_raw = content
# List of objects
histogram_list = []
for obj in histogram_raw:
    # Parse objects to tuples
    histogram_list.append(ast.literal_eval(obj))

idx = 0
for obj in histogram_list:
    # Parse tuples to dictionaries
    new_dict = {'id': int(obj[0]), 'count': obj[1]}
    histogram_list[idx] = new_dict
    idx += 1

# Sort by id
sorted_thresholds = sorted(histogram_list, key=lambda k: k['id'])
plot_threshold(sorted_thresholds)
