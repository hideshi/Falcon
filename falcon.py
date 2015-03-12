import sqlite3
import pickle
import re
from collections import OrderedDict
import pprint as pp

def log(method):
    def wrapper(self, *args):
        print('begin:', self.__class__.__name__, method.__name__)
        print(args)
        res = method(self, *args)
        print('end:', self.__class__.__name__, method.__name__)
        return res
    return wrapper

class Indexer(object):

    @log
    def __init__(self, database_file):
        self._database_file = database_file
        self._inverted_index = OrderedDict()
        self.stopwords = re.compile(r'[\s,.]')
        connection = sqlite3.connect(self._database_file)
        with connection:
            cursor = connection.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS indexes (
                      token TEXT PRIMARY KEY
                    , posting_list BLOB
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS documents (
                      id INTEGER PRIMARY KEY
                    , title TEXT
                    , content TEXT
                )
            """)

    @log
    def create_new_index(self):
        connection = sqlite3.connect(self._database_file)
        with connection:
            cursor = connection.cursor()
            cursor.execute('DELETE FROM indexes')

    @log
    def delete_documents(self):
        connection = sqlite3.connect(self._database_file)
        with connection:
            cursor = connection.cursor()
            cursor.execute('DELETE FROM documents')
        #SELECT FROM documents

    @log
    def add_index(self, title, content, document_id = 0):
        if(document_id == 0):
            document_id = self._store_document(title, content)
        self._create_posting_list(document_id, title, content)
        self._flush_buffer()

    @log
    def _store_document(self, title, content):
        connection = sqlite3.connect(self._database_file)
        with connection:
            cursor = connection.cursor()
            cursor.execute('INSERT INTO documents (title, content) VALUES(?, ?)', (title, content))
            lastrowid = cursor.lastrowid
            return lastrowid

    @log
    def _create_posting_list(self, document_id, title, content):
        document = title + content
        length = len(document)
        for i in range(0, length):
            token = document[i:i+2]
            m = self.stopwords.search(token)
            if len(token) == 2 and m == None:
                if token in self._inverted_index:
                    inverted_index_hash = self._inverted_index[token]
                    inverted_index_hash.add(document_id, i)
                else:
                    connection = sqlite3.connect(self._database_file)
                    with connection:
                        cursor = connection.cursor()
                        cursor.execute('SELECT token, posting_list FROM indexes WHERE token = ?', (token,))
                        rows = cursor.fetchall()
                        if len(rows) > 0:
                            for row in rows:
                                unpickled = pickle.loads(row[1])
                                unpickled.add(document_id, i)
                                self._inverted_index[token] = unpickled
                        else:
                            self._inverted_index[token] = InvertedIndexHash(token, document_id, i)

    @log
    def _flush_buffer(self):
        connection = sqlite3.connect(self._database_file)
        with connection:
            for k, v in self._inverted_index.items():
                pickled = pickle.dumps(v)
                cursor = connection.cursor()
                cursor.execute('INSERT OR REPLACE INTO indexes (token, posting_list) VALUES (?, ?)', (k, pickled))

class InvertedIndexHash(object):

    @log
    def __init__(self, token, document_id, position):
        self.token = token
        self.posting_list = OrderedDict()
        self.posting_list[document_id] = [position]
        self.positions_count = 1

    @log
    def add(self, document_id, position):
        if document_id in self.posting_list:
            pos = self.posting_list[document_id]
            pos.append(position)
            self.posting_list[document_id] = pos
            self.positions_count = self.positions_count + 1
        else:
            self.posting_list[document_id] = [position]
            self.positions_count = self.positions_count + 1

if __name__ == '__main__':
    indexer = Indexer('test.db')
    #indexer.create_new_index()
    #indexer.delete_documents()
    indexer.add_index('greeting', 'Good morning everyone.')
    indexer.add_index('introduction', 'My name is Taro.')
    
    connection = sqlite3.connect('test.db')
    with connection:
        cursor = connection.cursor()
        cursor.execute('SELECT id, title, content FROM documents')
        for row in cursor.fetchall():
            print(row[0], row[1], row[2])
        cursor.execute('SELECT token, posting_list FROM indexes ORDER BY token')
        for k1, v1 in cursor.fetchall():
            o = pickle.loads(v1)
            print(o.token, o.positions_count, o.posting_list)
