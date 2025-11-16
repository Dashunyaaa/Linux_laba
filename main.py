import SearchEngine
from sqlite3 import dbapi2 as sqlite

import nn

myNet = nn.searchnet()

pageList = ['https://stopgame.ru']


def main():
    crawler = SearchEngine.Crawler()
    # crawler.createIndexTables()

    # crawler.crawl(pageList)

    # print([row for row in crawler.con.execute('select rowid from wordlocation where wordid=1')])

    # crawler.calculatePageRank()

    e = SearchEngine.Searcher()

    # cur = crawler.con.execute("select * from pagerank order by score desc")
    # for i in range(3):
    #     print(cur.__next__())
    # print(e.getUrlName(3))

    # myNet.makeTables()

    q = e.query('xbox')

    if q is not None:
        (wordIDs, urlIDs) = q
        myNet.trainQuery(wordIDs, urlIDs, urlIDs[2])


if __name__ == "__main__":
    main()

