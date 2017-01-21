import ast
from collections import OrderedDict

import matplotlib.pyplot as plt
import numpy
from datetime import datetime, timedelta


def format_data(data, threshold):
    """
    Function that gets raw data (each row as string) from the cluster and formats it as a list of dictionaries with
    fields: id and count. The data should be in format [('2012-01-01!1',1),...]
    :param data: Raw data read from files.
    :param threshold: Threshold of the number of matches.
    :return: Formatted list and dictionary with date keys and counts for the correlation statistics.
    """
    count_list_raw = []
    for obj in data:
        # Parse objects to tuples
        count_list_raw.append(ast.literal_eval(obj))

    count_list = []
    for idx, obj in enumerate(count_list_raw):
        try:
            matches = int(obj[0][obj[0].index('!') + 1:len(obj[0])])
            idx_to_remove_start = obj[0].index('!')
            date_str = obj[0][0:idx_to_remove_start]
            datetime_object = datetime.strptime(date_str, '%Y-%m-%d')
            new_dict = {'date': datetime_object, 'count': obj[1], 'matches': matches}
            if matches >= threshold:
                count_list.append(new_dict)
            else:
                new_dict = {'date': datetime_object, 'count': 0, 'matches': matches}
                count_list.append(new_dict)
        except ValueError:
            pass

    count_list.sort(key=lambda x: x['date'])

    temp_dict = OrderedDict()
    for obj in count_list:
        if temp_dict.has_key(obj['matches']):
            temp_dict[obj['matches']] += obj['count']
        else:
            temp_dict[obj['matches']] = obj['count']
    temp_list = []
    for key, value in temp_dict.iteritems():
        temp_list.append({
            'id': key,
            'count': value
        })
    return count_list, temp_list


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
    plt.ticklabel_format(useOffset=False)
    # Logarithmic scale
    plt.yscale('log')
    stats = 'Minimum: ' + str(numpy.min(count)) \
            + '\n' + 'Maximum: ' + str(int(numpy.max(count))) \
            + '\n' + 'Average: ' + str(int(numpy.average(count)))
    plt.annotate(stats, xy=(1, 1), xycoords='axes fraction', fontsize=16, xytext=(-5, -5), textcoords='offset points',
                 ha='right', va='top')

    plt.show()


# Read file and assign
fname = './threshold_twitter_output/results.dat'
with open(fname) as f:
    content = f.readlines()

# Raw data
threshold_raw = content
# List of objects
threshold_list, matches_list = format_data(threshold_raw, 0)
# Sorts list of dicts
sorted_thresholds = sorted(matches_list, key=lambda k: k['id'])
# Plot the data
plot_threshold(sorted_thresholds)
