from math import tanh
import psycopg2


def dtanh(y):
    return 1.0 - y * y


class searchnet:
    def __init__(self):
        try:
            self.con = psycopg2.connect(f"dbname='nn' user='alexander' host='localhost' password='mint12345'")
        except (Exception) as error:
            print(error)

    def __del__(self):
        self.con.close()

    def makeTables(self):
        with self.con.cursor() as cur:
            try:
                cur.execute("create table hiddennode(rowid SERIAL PRIMARY KEY, create_key text)")
                cur.execute("create table wordhidden(rowid SERIAL PRIMARY KEY, fromid int, toid int, strength float)")
                cur.execute("create table hiddenurl(rowid SERIAL PRIMARY KEY, fromid int,toid int,strength float)")
            except (Exception, psycopg2.DatabaseError) as error:
                print(f"s.27: {error}");
        self.con.commit()

    def getStrength(self, fromID, toID, layer):
        if layer == 0:
            table = 'wordhidden'
        else:
            table = 'hiddenurl'

        with self.con.cursor() as cur:
            try:
                cur.execute(f"select strength from {table} where fromid={fromID} and toid={toID}")
                res = cur.fetchone()
            except (Exception, psycopg2.DatabaseError) as error:
                print(f"s.39: {error}");

        if res is None:
            if layer == 0:
                return -0.2

            if layer == 1:
                return 0

        return res[0]

    def setStrength(self, fromID, toID, layer, strength):
        if layer == 0:
            table = 'wordhidden'
        else:
            table = 'hiddenurl'

        with self.con.cursor() as cur:
            try:
                cur.execute(f"select rowid from {table} where fromid={fromID} and toid={toID}")
                res = cur.fetchone()

                if res is None:
                    cur.execute(f"insert into {table}(fromid,toid,strength) values ({fromID},{toID},{strength})")
                else:
                    rowID = res[0]
                    cur.execute(f"update {table} set strength={strength} where rowid={rowID}")
            except (Exception, psycopg2.DatabaseError) as error:
                print(f"s.66: {error}");

    def generateHiddenNode(self, wordIDs, urls):
        if len(wordIDs) > 3:
            return None

        createKey = "_".join(sorted([str(wi) for wi in wordIDs]))

        with self.con.cursor() as cur:
            try:
                cur.execute(f"select rowid from hiddennode where create_key='{createKey}'")
                res = cur.fetchone()
            except (Exception, psycopg2.DatabaseError) as error:
                print(f"s.81: {error}");

            if res is None:
                try:
                    cur.execute(f"insert into hiddennode(create_key) values ('{createKey}') returning rowid")
                    hiddenID = cur.fetchone()[0]
                except (Exception, psycopg2.DatabaseError) as error:
                    print(f"s.88: {error}");
                
                for wordID in wordIDs:
                    self.setStrength(wordID, hiddenID, 0, 1.0 / len(wordIDs))

                for urlID in urls:
                    self.setStrength(hiddenID, urlID, 1, 0.1)
            
        self.con.commit()

    def getAllHiddenIDs(self, wordIDs, urlIDs):
        l1 = {}
        with self.con.cursor() as cur:
            try:
                for wordID in wordIDs:
                    cur.execute(f"select toID from wordhidden where fromid={wordID}")
                    tmp = cur.fetchall()
                    for row in tmp:
                        l1[row[0]] = 1

                for urlID in urlIDs:
                    cur.execute(f"select fromid from hiddenurl where toID={urlID}")
                    tmp = cur.fetchall()
                    for row in tmp:
                        l1[row[0]] = 1
            except (Exception, psycopg2.DatabaseError) as error:
                print(f"s.111: {error}");

        return list(l1.keys())

    def setupNetwork(self, wordIDs, urlIDs):
        self.wordIDs = wordIDs
        self.hiddenIDs = self.getAllHiddenIDs(wordIDs, urlIDs)
        self.urlIDs = urlIDs

        self.ai = [1.0] * len(self.wordIDs)
        self.ah = [1.0] * len(self.hiddenIDs)
        self.ao = [1.0] * len(self.urlIDs)

        self.wi = [[self.getStrength(wordID, hiddenID, 0) for hiddenID in self.hiddenIDs] for wordID in self.wordIDs]
        self.wo = [[self.getStrength(hiddenID, urlID, 1) for urlID in self.urlIDs] for hiddenID in self.hiddenIDs]

    def feedforward(self):
        for i in range(len(self.wordIDs)):
            self.ai[i] = 1.0

        for j in range(len(self.hiddenIDs)):
            sum = 0.0

            for i in range(len(self.wordIDs)):
                sum = sum + self.ai[i] * self.wi[i][j]

            self.ah[j] = tanh(sum)

        for k in range(len(self.urlIDs)):
            sum = 0.0

            for j in range(len(self.hiddenIDs)):
                sum = sum + self.ah[j] * self.wo[j][k]

            self.ao[k] = tanh(sum)

        return self.ao[:]

    def getResult(self, wordIDs, urlIDs):
        self.setupNetwork(wordIDs, urlIDs)
        return self.feedforward()

    def backPropogate(self, targets, N=0.5):
        outputDeltas = [0.0] * len(self.urlIDs)

        for k in range(len(self.urlIDs)):
            error = targets[k] - self.ao[k]
            outputDeltas[k] = dtanh(self.ao[k]) * error

        hiddenDeltas = [0.0] * len(self.hiddenIDs)

        for j in range(len(self.hiddenIDs)):
            error = 0.0

            for k in range(len(self.urlIDs)):
                error = error + outputDeltas[k] * self.wo[j][k]

            hiddenDeltas[j] = dtanh(self.ah[j]) * error

        for j in range(len(self.hiddenIDs)):
            for k in range(len(self.urlIDs)):
                change = outputDeltas[k] * self.ah[j]
                self.wo[j][k] = self.wo[j][k] + N * change

        for i in range(len(self.wordIDs)):
            for j in range(len(self.hiddenIDs)):
                change = hiddenDeltas[j] * self.ai[i]
                self.wi[i][j] = self.wi[i][j] + N * change

    def trainQuery(self, wordIDs, urlIDs, selectedURL):
        self.generateHiddenNode(wordIDs, urlIDs)

        self.setupNetwork(wordIDs, urlIDs)
        self.feedforward()

        targets = [0.0] * len(urlIDs)
        targets[urlIDs.index(selectedURL)] = 1.0
        error = self.backPropogate(targets)

        self.updateDataBase()

    def updateDataBase(self):
        for i in range(len(self.wordIDs)):
            for j in range(len(self.hiddenIDs)):
                self.setStrength(self.wordIDs[i], self.hiddenIDs[j], 0, self.wi[i][j])

        for j in range(len(self.hiddenIDs)):
            for k in range(len(self.urlIDs)):
                self.setStrength(self.hiddenIDs[j], self.urlIDs[k], 1, self.wo[j][k])

        self.con.commit()
