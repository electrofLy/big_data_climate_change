#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import pyspark
from pyspark import sql
import re
from datetime import datetime

terms = {"Landbouw", "Alternatief", "Toegepast", "Wetenschap", "Bewustmaking", "Balans", "Gedrag", "Verandering",
         "Biodiversiteit", "Bioenergie", "Biomassa", "Biomimicry", "Brundtland", "Commissie", "Koolstof", "Offset",
         "Katalyseren", "Verandering", "Stad", "Civiel", "Klimaat", "Kust", "Samenwerking", "Gemeenschap", "Gezondheid",
         "Complexe", "Systemen", "Conserveren", "Biologie", "Maatschappelijk", "Kosten", "Baten", "Analyse", "Cultuur",
         "Ontbossing", "Design", "Ontwikkeling", "Ramp", "Discriminatie", "Diversiteit", "Ecologie", "Ecologisch",
         "Economisch", "Economische", "Ecosysteem", "Efficiëntie", "Energie", "Toegeweid", "Beurs", "Ondernemerschap",
         "Omgeving", "Gelijkheid", "Billijkheid", "Landbouw", "Financiën", "Voedel", "Voedselsystemen", "Voedselketen",
         "Spoor", "Toekomst", "Geslacht", "Geothermisch", "Geothermische", "Globaal", "Globalisering", "Bestuur",
         "Groen", "Broeikas", "Gas", "Groei", "Menselijk", "Voorwaarde", "Rechten", "Human", "Innovatie", "Innovatief",
         "Geïntegreerd", "Aansluitingen", "Interdisciplinair", "Investeren", "Justitie", "Land", "Landschap",
         "Leiderschap", "Leven", "Levenscyclus", "Lokale", "Marine", "Materialen", "Minderheid", "Modernisering",
         "Bewegingen", "Multidisciplinair", "Multidisciplinaire", "Natuurlijk", "Natuurlijke", "Systemen", "Natuur",
         "Voeding", "Overtreffen", "Partnerschap", "Photovoltaisch", "Ruimtelijk", "Beleid", "Politiek", "Politieke",
         "Bevolking", "Armoede", "Probleem", "Gebaseerd", "Welvaart", "Publiek", "Volksgezondheid", "Race", "Recycle",
         "Recycling", "Hernieuwbaare", "Hernieuwbaar", "Veerkracht", "Bronnen", "Draaibaar", "Fonds", "Rechten",
         "Landelijk", "Zeeniveau", "Sociale", "Zon", "Oplossingen", "Belanghebbende", "Stewardship", "Suburbanisatie",
         "Toevoer", "Ketting", "Duurzaamheid", "Duurzaam", "Duurzame", "Dynamieken", "Denken", "Technologie",
         "Afwegingen", "Transdisciplinair", "Transformatie", "Doorvoer", "Transparantie", "Transport", "Planet",
         "Ondergeserveerd", "Onbedoeld", "Onbedoelde", "Gevolgen", "Stedelijk", "Verstedelijking", "Water", "Welzijn",
         "Wind", "Energie"}
# Make all terms lowercase
terms = set(t.lower() for t in terms)

# The title factor for articles
TITLE_FACTOR = 1
THRESHOLD_VOLKSKRANT = 9
THRESHOLD_TWITTER = 3


def getTermCount(string):
    """
    Counts the number of terms in the string

    Validates the string.
    Makes it lowercase
    Removes all non-alpha characters
    Splits into words
    Counts the number of occurrences of any term in the list
    """
    if string is None or string == '':
        return 0

    # Lowercase
    string = string.lower()

    # Remove strange chars
    string = re.sub('[^a-z ]+', '', string)

    # Split into words
    words = string.split(' ')

    count = 0
    for w in words:
        if w in terms:
            count += 1

    return count


def length(string):
    """
    Check the length of the string. 0 if string is None
    """
    if string is None:
        return 0
    return len(string)


def per1000Words(count, leng):
    """
    Calculates the p1000 value: the number of matches per 1000 words.
    If either the count or the length is 0, then 0 is returned.
    """
    if count == 0 or leng == 0:
        return 0

    return int(round(count * 1000.0 / leng, 0))


def createVolkskrantKey(row, totalCount):
    """
    Create a Volkskrant key: the date and the count
    Output like 2016-11-01!4
    """
    return str(row.year) + '-' + str(row.month) + '-' + str(row.day) + '!' + str(totalCount)


def createTwitterKey(row, count):
    """
    Create a Twitter key: the date and the count.
    Checks if the 'created_at' key exists: not all data has the same structure.
    The timezone is removed from the parsed timestamp string,
        because Python 2.7 has problems with the %z datetime directive.
    Output like 2016-11-01!4

    If no valid timestamp key is found, '!' with the count is returned.
    """
    if row['created_at'] is not None:
        fmt = '%a %b %d %X %Y'
        d = datetime.strptime(row['created_at'][:-11] + row['created_at'][-5:], fmt).date()
        return d.isoformat() + '!' + str(count)

    return '!' + str(count)


def mapVolskrantFun(row):
    """
    Maps the row to the Volkskrant key and 1.
    Optionally the TITLE_FACTOR constant can be used to increase the influence the title has on the count.
    The count is calculated as a p1000 value: the number of matches of terms per 1000 words in the article.
    """
    totalCount = getTermCount(row.title) * TITLE_FACTOR + getTermCount(row.text)
    totalLength = length(row.title) + length(row.text)
    p1000 = per1000Words(totalCount, totalLength)
    return createVolkskrantKey(row, p1000), 1


def mapTwitterFun(row):
    """
    Maps the row to the Twitter key and 1 to count the tweets for this key, and for later grouping and processing
    """
    count = getTermCount(row.text)
    return createTwitterKey(row, count), 1


def processData(data):
    """
    Processes the data to the required format to perform statistics on

    Map to key and count
    Add all counts by key
    Sort the result
    """
    return data \
        .map(mapVolskrantFun) \
        .reduceByKey(lambda a, b: a + b) \
        .sortByKey()


def findTop(data):
    """
    Find the top rows to analyse the contents

    Drop a column which is not required
    Map the row to the count
    Filter rows with counts below the threshold
    """
    return data \
        .drop('full_html') \
        .map(lambda r: (r, getTermCount(r.text))) \
        .filter(lambda r: r[1] >= THRESHOLD_VOLKSKRANT)


def main():
    """
    Main function

    Parses arguments
    Creates configuration
    Creates Spark (SQL) context
    Processes data
    """

    # parse arguments
    in_dir, out_dir = sys.argv[1:]

    conf = pyspark.SparkConf().setAppName("CountVolkskrantYear %s %s" % (in_dir, out_dir))
    sc = pyspark.SparkContext(conf=conf)

    sqlContext = sql.SQLContext(sc)

    # invoke job and put into output directory
    processData(sqlContext.read.json(in_dir)).saveAsTextFile(out_dir)


if __name__ == '__main__':
    main()
