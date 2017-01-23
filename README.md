# University of Twente - Project for course Managing Big Data on topic "RELATION BETWEEN DUTCH NEWS COVERAGE AND CLIMATE AWARENESS ON TWITTER"

Developed by:
- Ivaylo Hristov
- Hidde Wieringa
- Sebastian Panman de Wit

## Data

Twitter data of Tjong Kim Sang, E., Van den Bosch, A., Dealing with big data: The case of Twitter, Computational Linguistics in the Netherlands Journal 3 (2013) 121-134.

Volkskrant data has been scraped.

The time period is the full years from 2011 until 2015.

Process data using `spark/CountVolkskrantYear.py` on a Spark (SQL) instance.

Different output of this script has been put in the folders `plotting/*_output` and `plotting/*_count`.

## Visualization

Visualization of the output:

- `plotting/combine_files.py` can be used to combine mutliple text files into one.
- `plotting/data_plot.py` can be used to plot Spark output.
- `plotting/histogram_plot.py` can be used to plot Spark output as a histogram.
- `plotting/threshold_plotting.py` can be used to plot a summary of the Volkskrant output to determine the threshold.
- `plotting/threshold_twitter.py` can be used to plot a summary of the Twitter output to determine the threshold.
