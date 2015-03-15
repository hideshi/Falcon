import sqlite3
import pickle
import re
import argparse
import unittest

def log(method):
    def wrapper(self, *args):
        if IndexManager.debug:
            print('begin:', self.__class__.__name__, method.__name__)
            print(args)
        result = method(self, *args)
        if IndexManager.debug:
            print('end:', self.__class__.__name__, method.__name__)
        return result
    return wrapper

class Tokenizer(object):
    @log
    def __init__(self):
        self.stopwords = re.compile(r'[\s,.!\?"\'\$%&#:;\{\}\[\]\(\)、。（）]')

    @log
    def tokenize(self, title, content):
        raise NotImplementedError("tokenize method must be overridden and implemented by a descendant class.")

class BigramTokenizer(Tokenizer):
    @log
    def tokenize(self, title, content = None):
        if content != None:
            document = title + content
        else:
            document = title
        length = len(document)
        tokens = []
        for i in range(0, length):
            token = document[i:i+2]
            m = self.stopwords.search(token)
            if len(token) == 2 and m == None:
                tokens.append((i, token))
        return tokens

class TrigramTokenizer(Tokenizer):
    @log
    def tokenize(self, title, content = None):
        if content != None:
            document = title + content
        else:
            document = title
        length = len(document)
        tokens = []
        for i in range(0, length):
            token = document[i:i+3]
            m = self.stopwords.search(token)
            if len(token) == 3 and m == None:
                tokens.append((i, token))
        return tokens

class TokenizerFactory(object):
    @log
    def create_tokenizer(self, tokenizer):
        module = __import__("falcon")
        class_name = tokenizer + 'Tokenizer'
        clazz = None
        try:
            clazz = getattr(module, class_name)
        except AttributeError:
            print(class_name + ' is not implemented.')
            exit(1)
        return clazz()

class Indexer(object):
    @log
    def __init__(self, database_file, tokenizer_type = 'Bigram'):
        self._database_file = database_file
        self._tokenizer = TokenizerFactory().create_tokenizer(tokenizer_type)
        self._inverted_index = {}
        connection = sqlite3.connect(self._database_file)
        with connection:
            cursor = connection.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS indices (
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
    def delete_index(self):
        connection = sqlite3.connect(self._database_file)
        with connection:
            cursor = connection.cursor()
            cursor.execute('DELETE FROM indices')

    @log
    def delete_documents(self):
        connection = sqlite3.connect(self._database_file)
        with connection:
            cursor = connection.cursor()
            cursor.execute('DELETE FROM documents')
        self.delete_index()

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
        for i, token in self._tokenizer.tokenize(title, content):
            if token in self._inverted_index:
                inverted_index_hash = self._inverted_index[token]
                inverted_index_hash.add(document_id, i)
            else:
                connection = sqlite3.connect(self._database_file)
                with connection:
                    cursor = connection.cursor()
                    cursor.execute('SELECT token, posting_list FROM indices WHERE token = ?', (token,))
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
                cursor.execute('INSERT OR REPLACE INTO indices (token, posting_list) VALUES (?, ?)', (k, pickled))

class InvertedIndexHash(object):
    @log
    def __init__(self, token, document_id, position):
        self.token = token
        self.posting_list = {}
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

class Searcher(object):
    @log
    def __init__(self, database_file, tokenizer_type = 'Bigram'):
        self._database_file = database_file
        self._tokenizer = TokenizerFactory().create_tokenizer(tokenizer_type)

    @log
    def search(self, word):
        tokens = self._tokenizer.tokenize(word)
        connection = sqlite3.connect(self._database_file)
        documents = {}
        for i, token in tokens:
            with connection:
                cursor = connection.cursor()
                cursor.execute('SELECT token, posting_list FROM indices WHERE token = ?', (token,))
                rows = cursor.fetchall()
                if len(rows) > 0:
                    for row in rows:
                        unpickled = pickle.loads(row[1])
                        for k, v in unpickled.posting_list.items():
                            if k not in documents:
                                documents[k] = []
                            for i in v:
                                documents[k].append((i, token))
                            documents[k] = sorted(documents[k])
        return self._get_documents(self._get_matched_document_ids(documents, tokens))

    @log
    def _get_matched_document_ids(self, documents, tokens):
        matched_document_ids = []
        for document_id, positions in documents.items():
            sorted_positions = sorted(positions)
            number_of_tokens = len(tokens)
            sequence = 1
            prev_position = -1
            for position, token in sorted_positions:
                if position - prev_position != 1:
                    sequence = 1
                if sequence == 1 or (sequence <= number_of_tokens and position - prev_position == 1):
                    t = tokens[sequence-1][1]
                    if token == t:
                        if number_of_tokens == sequence:
                            matched_document_ids.append(document_id)
                        sequence = sequence + 1
                prev_position = position
        return set(matched_document_ids)

    def _get_documents(self, matched_document_ids):
        if len(matched_document_ids) == 0:
            return {}
        connection = sqlite3.connect(self._database_file)
        with connection:
            cursor = connection.cursor()
            cursor.execute('SELECT id, title, content FROM documents WHERE id IN({0})'.format(', '.join(str(i) for i in matched_document_ids)))
            return cursor.fetchall()
        
        
class TokenizerFactoryTest(unittest.TestCase):
    def runTest(self):
        self.test_create_tokenizer()

    def test_create_tokenizer(self):
        o = TokenizerFactory()
        self.assertEqual(o.create_tokenizer('Bigram').__class__.__name__, BigramTokenizer().__class__.__name__)
        self.assertEqual(o.create_tokenizer('Trigram').__class__.__name__, TrigramTokenizer().__class__.__name__)

class BigramTokenizerTest(unittest.TestCase):
    def runTest(self):
        self.test_tokenize()

    def test_tokenize(self):
        o = BigramTokenizer()
        self.assertEqual(o.tokenize('abcd'), [(0, 'ab'), (1, 'bc'), (2, 'cd')])
        self.assertEqual(o.tokenize('a cd'), [(2, 'cd')])

class TrigramTokenizerTest(unittest.TestCase):
    def runTest(self):
        self.test_tokenize()

    def test_tokenize(self):
        o = TrigramTokenizer()
        self.assertEqual(o.tokenize('abcde'), [(0, 'abc'), (1, 'bcd'), (2, 'cde')])
        self.assertEqual(o.tokenize('ab def'), [(3, 'def')])

class SearcherTest(unittest.TestCase):
    def runTest(self):
        self.test__get_matched_document_ids()

    def test__get_matched_document_ids(self):
        o = Searcher("", "Bigram")
        self.assertEqual(o._get_matched_document_ids({1 : [(0, 'ab'), (1, 'bc'), (2, 'cd'), (3, 'de')], 2 : [(0, 'bc'), (1, 'ce'), (2, 'ef')]}, [(0, 'bc'), (1, 'cd')]), {1})
        self.assertEqual(o._get_matched_document_ids({1 : [(0, 'ab'), (1, 'bc'), (2, 'cd'), (3, 'de')], 2 : [(0, 'bc'), (1, 'ce'), (2, 'ef')]}, [(0, 'ce'), (1, 'ef')]), {2})
        self.assertEqual(o._get_matched_document_ids({1 : [(0, 'ab'), (1, 'bc'), (2, 'cd'), (3, 'de')], 2 : [(0, 'bc'), (1, 'ce'), (2, 'ef')]}, [(0, 'bc')]), {1, 2})

class IndexManager(object):
    
    debug = False

    test_classes = (SearcherTest, TokenizerFactoryTest, BigramTokenizerTest, TrigramTokenizerTest)

    @log
    def run(self):
        parser = argparse.ArgumentParser(description='Falcon Full Text Search Engine')
        parser.add_argument('-D', '--debug', help='enable debug mode', action='store_true')
        parser.add_argument('-T', '--test', help='run test', action='store_true')
        parser.add_argument('-I', '--showindex', help='show index', action='store_true')
        parser.add_argument('-C', '--showdocument', help='show document(s)', action='store_true')
        parser.add_argument('-c', '--content', metavar='content', help='document content to be stored and indexed')
        parser.add_argument('-d', '--databasefile', metavar='databasefile', help='a sqlite3 database file')
        parser.add_argument('-q', '--query', metavar='query', help='query string')
        parser.add_argument('-t', '--title', metavar='title', help='document title to be stored and indexed')
        parser.add_argument('-z', '--tokenizer', metavar='tokenizer', help='Type of tokenizer [Bigram, Trigram]')
        parser.add_argument('files', metavar='files', nargs='*', help='input file(s)')
        self._args = parser.parse_args()

        IndexManager.debug = self._args.debug

        if self._args.test:
            suite = unittest.TestSuite()
            for test_class in IndexManager.test_classes:
                suite.addTest(test_class())
            runner = unittest.TextTestRunner()
            runner.run(suite)

        if self._args.databasefile != None:
            if self._args.query != None:
                if self._args.tokenizer != None:
                    searcher = Searcher(self._args.databasefile, self._args.tokenizer)
                else:
                    searcher = Searcher(self._args.databasefile)
                search_results = searcher.search(self._args.query)
                for result in search_results:
                    print(result)
            elif self._args.title != None and self._args.content != None:
                if self._args.tokenizer != None:
                    indexer = Indexer(self._args.databasefile, self._args.tokenizer)
                else:
                    indexer = Indexer(self._args.databasefile)
                indexer.add_index(self._args.title, self._args.content)

            if self._args.showindex:
                connection = sqlite3.connect(self._args.databasefile)
                with connection:
                    cursor = connection.cursor()
                    cursor.execute('SELECT token, posting_list FROM indices ORDER BY token')
                    for k1, v1 in cursor.fetchall():
                        o = pickle.loads(v1)
                        print(o.token, o.positions_count, o.posting_list)

            if self._args.showdocument:
                connection = sqlite3.connect(self._args.databasefile)
                with connection:
                    cursor = connection.cursor()
                    cursor.execute('SELECT id, title, content FROM documents')
                    for row in cursor.fetchall():
                        print(row[0], row[1], row[2])

if __name__ == '__main__':
    index_manager = IndexManager()
    index_manager.run()
