import urllib.request
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import sqlite3
import re
import nn

mynet=nn.searchnet('nn.db')

ignorewords = set(['the', 'of', 'to', 'and', 'a', 'in', 'is', 'it'])


class Crawler:
    # Инициализация паука, передав ему имя базы данных
    def __init__(self, dbname):
        self.con = sqlite3.connect(dbname)

    def __del__(self):
        self.con.close()

    def dbcommit(self):
        self.con.commit()

    # Вспомогательная функция для добавления или получения идентификатора
    def getentryid(self, table, field, value, createnew=True):
        cur = self.con.execute(f"SELECT rowid FROM {table} WHERE {field}=?", (value,))
        res = cur.fetchone()
        if res is None:
            if createnew:
                cur = self.con.execute(f"INSERT INTO {table} ({field}) VALUES (?)", (value,))
                return cur.lastrowid
            else:
                return None
        else:
            return res[0]

    # Индексирование одной страницы
    def addtoindex(self, url, soup):
        if self.isindexed(url):
            return
        print(f'Индексируется {url}')

        text = self.gettextonly(soup)
        words = self.separatewords(text)
        urlid = self.getentryid('urllist', 'url', url)

        for i in range(len(words)):
            word = words[i]
            if word in ignorewords:
                continue
            wordid = self.getentryid('wordlist', 'word', word)
            self.con.execute(
                "INSERT INTO wordlocation(urlid, wordid, location) VALUES (?, ?, ?)",
                (urlid, wordid, i)
            )

    # Извлечение текста из HTML-страницы
    def gettextonly(self, soup):
        v = soup.string
        if v is None:
            c = soup.contents
            resulttext = ''
            for t in c:
                subtext = self.gettextonly(t)
                resulttext += subtext + '\n'
            return resulttext
        else:
            return v.strip()

    # Разбиение текста на слова
    def separatewords(self, text):
        splitter = re.compile(r'\W+')
        return [s.lower() for s in splitter.split(text) if s != '']

    # Возвращает True, если данный URL уже проиндексирован
    def isindexed(self, url):
        u = self.con.execute("SELECT rowid FROM urllist WHERE url=?", (url,)).fetchone()
        if u is not None:
            v = self.con.execute("SELECT * FROM wordlocation WHERE urlid=?", (u[0],)).fetchone()
            if v is not None:
                return True
        return False

    # Добавление ссылки с одной страницы на другую
    def addlinkref(self, urlFrom, urlTo, linkText):
        fromid = self.getentryid('urllist', 'url', urlFrom)
        toid = self.getentryid('urllist', 'url', urlTo)
        if fromid == toid:
            return
        cur = self.con.execute("INSERT INTO link(fromid, toid) VALUES (?, ?)", (fromid, toid))
        linkid = cur.lastrowid
        words = self.separatewords(linkText)
        for word in words:
            if word in ignorewords:
                continue
            wordid = self.getentryid('wordlist', 'word', word)
            self.con.execute("INSERT INTO linkwords(wordid, linkid) VALUES (?, ?)", (wordid, linkid))

    # Поиск в ширину до заданной глубины, индексируя все встречающиеся по пути
    def crawl(self, pages, depth=2):
        for i in range(depth):
            newpages = set()
            for page in pages:
                try:
                    # Добавьте headers, если сайт блокирует
                    req = urllib.request.Request(page, headers={'User-Agent': 'Mozilla/5.0'})
                    c = urllib.request.urlopen(req)
                except Exception as e:
                    print(f"Не могу открыть {page}: {e}")
                    continue
                soup = BeautifulSoup(c.read(), "html.parser")
                self.addtoindex(page, soup)

                links = soup('a')
                for link in links:
                    if 'href' in link.attrs:
                        url = urljoin(page, link['href'])
                        if url.find("'") != -1:
                            continue
                        url = url.split('#')[0]  # удалить часть URL после #
                        if url[0:4] == 'http' and not self.isindexed(url):
                            newpages.add(url)
                        linkText = self.gettextonly(link)
                        self.addlinkref(page, url, linkText)
                self.dbcommit()
            pages = newpages

    # Создание таблиц в базе данных
    def createindextables(self):
        self.con.execute('drop table if exists urllist')
        self.con.execute('drop table if exists wordlist')
        self.con.execute('drop table if exists wordlocation')
        self.con.execute('drop table if exists link')
        self.con.execute('drop table if exists linkwords')
        self.con.execute('drop table if exists pagerank')

        self.con.execute('create table urllist(url)')
        self.con.execute('create table wordlist(word)')
        self.con.execute('create table wordlocation(urlid, wordid, location)')
        self.con.execute('create table link(fromid integer, toid integer)')
        self.con.execute('create table linkwords(wordid, linkid)')
        self.con.execute('create table pagerank(urlid integer, score real)')
        self.con.execute('create index wordidx on wordlist(word)')
        self.con.execute('create index urlidx on urllist(url)')
        self.con.execute('create index urltoidx on link(toid)')
        self.con.execute('create index urlfromidx on link(fromid)')
        self.dbcommit()

    def calculatepagerank(self, iterations=20):
        # Инициализация PageRank = 1.0
        self.con.execute("DELETE FROM pagerank")
        self.con.execute("INSERT INTO pagerank(urlid, score) SELECT rowid, 1.0 FROM urllist")
        self.dbcommit()

        for i in range(iterations):
            print(f"Итерация PageRank {i}")
            for (urlid,) in self.con.execute("SELECT rowid FROM urllist"):
                pr = 0.15
                # Вклад всех ссылок
                for (linker,) in self.con.execute("SELECT fromid FROM link WHERE toid=?", (urlid,)):
                    linkingpr = self.con.execute("SELECT score FROM pagerank WHERE urlid=?", (linker,)).fetchone()[0]
                    linkingcount = self.con.execute("SELECT COUNT(*) FROM link WHERE fromid=?", (linker,)).fetchone()[0]
                    if linkingcount > 0:  # Избежать деления на 0
                        pr += 0.85 * (linkingpr / linkingcount)
                self.con.execute("UPDATE pagerank SET score=? WHERE urlid=?", (pr, urlid))
            self.dbcommit()


class Searcher:
    def __init__(self, dbname):
        self.con = sqlite3.connect(dbname)

    def __del__(self):
        self.con.close()

    # Поиск совпадений для слов запроса
    def getmatchrows(self, query):
        fieldlist = 'w0.urlid'
        tablelist = ''
        clauselist = ''
        wordids = []

        words = query.split(' ')
        tablenumber = 0

        for word in words:
            wordrow = self.con.execute("SELECT rowid FROM wordlist WHERE word=?", (word,)).fetchone()
            if wordrow is not None:
                wordid = wordrow[0]
                wordids.append(wordid)
                if tablenumber > 0:
                    tablelist += ','
                    clauselist += f' AND w{tablenumber - 1}.urlid=w{tablenumber}.urlid AND '
                fieldlist += f', w{tablenumber}.location'
                tablelist += f'wordlocation w{tablenumber}'
                clauselist += f'w{tablenumber}.wordid={wordid}'
                tablenumber += 1

        if tablelist == '':  # Если нет слов, вернуть пустой результат
            return [], []

        fullquery = f"SELECT {fieldlist} FROM {tablelist} WHERE {clauselist}"
        cur = self.con.execute(fullquery)
        rows = [row for row in cur]
        return rows, wordids

    # Ранжирование результатов
    def getscoredlist(self, rows, wordids):
        totalscores = dict([(row[0], 0) for row in rows])
        weights = [
            (1.0, self.frequencyscore(rows)),
            (1.0, self.locationscore(rows)),
            (1.0, self.distancescore(rows)),
            (1.0, self.inboundlinkscore(rows)),
            (1.0, self.pagerankscore(rows)),
            (1.0, self.linktextscore(rows, wordids)),
            (1.0, self.nnscore(rows, wordids))
        ]

        for (weight, scores) in weights:
            for url in totalscores:
                totalscores[url] += weight * scores.get(url, 0)

        return totalscores

    def geturlname(self, id):
        return self.con.execute("SELECT url FROM urllist WHERE rowid=?", (id,)).fetchone()[0]

    # Выполнение запроса
    def query(self, q):
        rows, wordids = self.getmatchrows(q)
        scores = self.getscoredlist(rows, wordids)
        rankedscores = sorted([(score, url) for (url, score) in scores.items()], reverse=True)
        for (score, urlid) in rankedscores[0:10]:
            print(f"{score:.3f}\t{self.geturlname(urlid)}")
        return wordids, [r[1] for r in rankedscores[0:10]]

    def normalizescores(self, scores, smallIsBetter=0):
        vsmall = 0.00001  # Предотвратить деление на нуль
        if smallIsBetter:
            minscore = min(scores.values()) if scores else vsmall
            return dict([(u, float(minscore) / max(vsmall, scores[u])) for u in scores])
        else:
            maxscore = max(scores.values())
            if maxscore == 0: maxscore = vsmall
            return dict([(u, float(c) / maxscore) for (u, c) in scores.items()])

    def frequencyscore(self, rows):
        counts = dict([(row[0], 0) for row in rows])
        for row in rows:
            counts[row[0]] += 1
        return self.normalizescores(counts)

    def locationscore(self, rows):
        locations = dict([(row[0], 1000000) for row in rows])
        for row in rows:
            loc = sum(row[1:])
            if loc < locations[row[0]]:
                locations[row[0]] = loc
        return self.normalizescores(locations, smallIsBetter=1)

    def distancescore(self, rows):
        # Если есть только одно слово, любой документ выигрывает!
        if len(rows[0]) <= 2:
            return dict([(row[0], 1.0) for row in rows])

        # Инициализировать словарь большими значениями
        mindistance = dict([(row[0], 1000000) for row in rows])

        for row in rows:
            dist = sum([abs(row[i] - row[i-1]) for i in range(2, len(row))])
            if dist < mindistance[row[0]]:
                mindistance[row[0]] = dist
        return self.normalizescores(mindistance, smallIsBetter=1)

    def inboundlinkscore(self, rows):
        uniqueurls = set([row[0] for row in rows])
        inboundcount = dict([(u, self.con.execute(
            'SELECT COUNT(*) FROM link WHERE toid=?', (u,)
        ).fetchone()[0]) for u in uniqueurls])
        return self.normalizescores(inboundcount)

    def pagerankscore(self, rows):
        pageranks = dict([(row[0], self.con.execute(
            'SELECT score FROM pagerank WHERE urlid=?', (row[0],)
        ).fetchone()[0]) for row in rows])
        maxrank = max(pageranks.values())
        normalizedscores = dict([(u, float(l) / maxrank) for (u, l) in pageranks.items()])
        return normalizedscores

    def linktextscore(self, rows, wordids):
        linkscores = dict([(row[0], 0) for row in rows])
        for wordid in wordids:
            cur = self.con.execute(
                'SELECT link.fromid, link.toid FROM linkwords, link '
                'WHERE wordid=? AND linkwords.linkid=link.rowid', (wordid,)
            )
            for (fromid, toid) in cur:
                if toid in linkscores:
                    pr = self.con.execute(
                        'SELECT score FROM pagerank WHERE urlid=?', (fromid,)
                    ).fetchone()[0]
                    linkscores[toid] += pr
        maxscore = max(linkscores.values())
        normalizedscores = dict([(u, float(l) / maxscore) for (u, l) in linkscores.items()])
        return normalizedscores

    def nnscore(self, rows, wordids):
        # Получить уникальные идентификаторы URL в виде упорядоченного списка
        urlids = [urlid for urlid in set([row[0] for row in rows])]
        nnres = mynet.getResult(wordids, urlids)
        scores = dict([(urlids[i], nnres[i]) for i in range(len(urlids))])
        return self.normalizescores(scores)


# Пример запуска
if __name__ == "__main__":
    crawler = Crawler("search.db")
    # crawler.createindextables()  # Раскомментируйте для создания таблиц
    # crawler.crawl(["https://www.woman.ru/forum/"], depth=2)  # Раскомментируйте для краулинга
    # crawler.calculatepagerank()  # Раскомментируйте для расчёта PageRank
    searcher = Searcher("search.db")
    q = searcher.query("мужчина")

    if q is not None:
        (wordIDs, urlIDs) = q
        mynet.trainQuery(wordIDs, urlIDs, urlIDs[0])

