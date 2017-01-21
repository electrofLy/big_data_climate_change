import simplejson
import datetime.datetime
import matplotlib.pyplot as plt


def plotdata(yearsNews, countsNews):
    # lineOne, = plt.plot(yearsTweets, countsTweets, label='Tweets')
    lineTwo, = plt.plot(yearsNews, countsNews, label='News')

    plt.ticklabel_format(useOffset=False)
    plt.legend(handles=[lineTwo], loc=2)
    # plt.xticks(range(min(yearsNews), max(yearsNews) + 1, 1))
    plt.xlabel('Date')
    plt.ylabel('Count')
    plt.show()

with open('./climate_articles_volkskrant_output/articles.json') as data_file:
    data = simplejson.load(data_file)

articles = data['data']
years = []
for article in articles:
    years.append((str(article['year']) + ' ' + str(article['month']),1))
#
# years.sort()
# testDict = defaultdict(int)
# for key,val in years:
#     testDict[key] += val
a = {}
for key,val in years:
    try:
        a[key] += 1
    except KeyError:
        a[key] = 1

print(a)
# itemchita = testDict.items()

yearsNews = []
countNews = []

for dateT in a:
    # datetime.datetime.strptime('24052010', "%d%m%Y").date()
    yearsNews.append(datetime.datetime.strptime(dateT[0],'%Y '))
    countNews.append(dateT[1])
#
plotdata(yearsNews,countNews)
# print(years)

