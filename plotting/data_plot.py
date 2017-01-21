from collections import OrderedDict
from datetime import datetime
from matplotlib import dates
from datetime import timedelta
import ast
import matplotlib.pyplot as plt
import numpy

# Threshold for tweets and news
THRESHOLD_TWEETS = 3
THRESHOLD_NEWS = 5


def format_data(data, threshold):
    """
    Function that gets raw data (each row as string) from the cluster and formats it as a list of dictionaries with
    fields: date, count and matches. The data should be in format [('2012-01-01!1',1),...]
    :param data: Raw data read from files.
    :param threshold: Threshold of the number of matches.
    :return: Formatted list and dictionary with date keys and counts for the correlation statistics.
    """
    count_list_raw = []
    for obj in data:
        # Parse objects to tuples
        count_list_raw.append(ast.literal_eval(obj))

    count_list = []
    # Format the data as list of dicts
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

    # Sort by date
    count_list.sort(key=lambda x: x['date'])

    # Dict with all the dates and their counts
    temp_dict = OrderedDict()
    for obj in count_list:
        if datetime.strptime('2011-01-16', '%Y-%m-%d') <= obj['date'] <= datetime.strptime('2015-12-5',
                                                                                           '%Y-%m-%d'):
            if obj['matches'] >= threshold:
                if temp_dict.has_key(obj['date']):
                    temp_dict[obj['date']] += obj['count']
                else:
                    temp_dict[obj['date']] = obj['count']
    date_start = datetime.strptime('2011-01-16', '%Y-%m-%d')
    date_end = datetime.strptime('2015-11-30', '%Y-%m-%d')
    temp_list = []
    for single_date in (date_start + timedelta(n) for n in range((date_end - date_start).days)):
        if temp_dict.has_key(single_date):
            temp_list.append(temp_dict[single_date])
        else:
            temp_list.append(0)

    return count_list, temp_list


def format_plots(data):
    """
    Function formatting list with dictionaries for the matplotlib.
    :param data: List of dictionaries formatted with fields date,count and matches
    :return: Two lists - one with the dates formatted for the plot and one for the number of counts
    """
    data_dates = []
    counts = []
    for data_obj in data:
        if datetime.strptime('2011-01-16', '%Y-%m-%d') <= data_obj['date'] <= datetime.strptime('2015-11-30',
                                                                                                '%Y-%m-%d'):
            data_dates.append(data_obj['date'])
            counts.append(data_obj['count'])
    twitter_dates = dates.date2num(data_dates)
    return twitter_dates, counts


def plot_data(data_f, data_l, corr_data_f, corr_data_l):
    """
    Plotting the actual data with matplotlib.
    :param data_f: Formatted data as list of dictionaries (date, count and matches) for the first line.
    :param data_l: Formatted data as list of dictionaries (date, count and matches) for the second line.
    :param corr_data_f: Data for calculating the correlation factor.
    :param corr_data_l: Data for calculating the correlation factor.
    :return: None
    """
    volkskrant_dates, volkskrant_counts = format_plots(data_f)
    twitter_dates, twitter_counts = format_plots(data_l)
    # Used 'b-' param so lines instead of dots are visualised
    l_r, = plt.plot_date(twitter_dates, twitter_counts, 'b-', color='r', label='Twitter')
    l_b, = plt.plot_date(volkskrant_dates, volkskrant_counts, 'b-', color='b', label='Volkskrant')
    plt.xlabel('Date')
    plt.ylabel('Count')
    plt.xticks(rotation=90)
    corr_coef = numpy.corrcoef(corr_data_f, corr_data_l)
    stats = 'Minimum tweets/day: ' + str(numpy.min(twitter_counts)) \
            + '\n' + 'Maximum tweets/day: ' + str(numpy.max(twitter_counts)) \
            + '\n' + 'Average tweets/day: ' + str(numpy.average(twitter_counts)) \
            + '\n' + 'Standard deviation tweets: ' + str(numpy.std(twitter_counts)) \
            + '\n' + 'Sum tweets: ' + str(sum(twitter_counts)) \
            + '\n' + 'Threshold tweets:' + str(THRESHOLD_TWEETS) \
            + '\n\n Minimum volkskrant/day: ' + str(numpy.min(volkskrant_counts)) \
            + '\n' + 'Maximum volkskrant/day: ' + str(numpy.max(volkskrant_counts)) \
            + '\n' + 'Average volkskrant/day: ' + str(numpy.average(volkskrant_counts)) \
            + '\n' + 'Standard deviation volkskrant: ' + str(numpy.std(volkskrant_counts)) \
            + '\n' + 'Sum volkskrant: ' + str(numpy.sum(volkskrant_counts)) \
            + '\n' + 'Threshold volkskrant:' + str(THRESHOLD_NEWS) \
            + '\n\n' + 'Correlation coefficient:' + str(corr_coef[0][1])
    # Add text to the plot
    plt.annotate(stats, xy=(1, 1), xycoords='axes fraction', fontsize=16, xytext=(-5, -5), textcoords='offset points',
                 ha='right', va='top')
    plt.legend(handles=[l_r, l_b], loc=2)
    plt.grid(True)
    plt.show()


# Files with data
fname_news = './volkskrant_count/data.dat'
fname_twitter = './twitter_count/results.dat'

# Open file
with open(fname_news) as f:
    content_news = f.readlines()

# Format the data
volkskrant_count_list, corr_data_volkskrant = format_data(content_news, THRESHOLD_NEWS)

# Open file
with open(fname_twitter) as f:
    content_twitter = f.readlines()

# Format the data
twitter_count_list, corr_data_twitter = format_data(content_twitter, THRESHOLD_TWEETS)

# Plot the data
plot_data(volkskrant_count_list, twitter_count_list, corr_data_volkskrant, corr_data_twitter)
