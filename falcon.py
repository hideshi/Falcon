#!/usr/bin/env python3
import unittest, json, gc
from sqlite3 import connect
from pickle import loads, dumps
from bz2 import compress, decompress
from re import compile, split
from argparse import ArgumentParser
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs
from datetime import datetime

_DEFAULT_TOKENIZER = 'Bigram'
_COMPRESS_LEVEL = 9
_DEFAULT_PORT = 8888
_TOKEN_POSITION_LIMIT = 5000000

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
        self.stopwords = compile(r'[0-9\s,.!?"\'$%&\-+=/#:;{}\[\]()<>\^~_→i｡@･ﾞ､｢｣…★☆♭\\–▼♪⇔♥°‐――≠※∞◇×、。（）：；「」『』【】［］｛｝〈〉《》〔〕〜～�｜｀＼＠？！”＃＄％＆’＝＋＊＜＞＿＾￥／，・´ ▽ ．－￤]')

    @log
    def tokenize(self, title, content):
        raise NotImplementedError("tokenize method must be overridden and implemented by a descendant class.")

class BigramTokenizer(Tokenizer):
    @log
    def tokenize(self, title, content = ''):
        document = title + content
        tokens = []
        for i in range(0, len(document)):
            token = document[i:i+2]
            if len(token) == 2 and self.stopwords.search(token) == None:
                tokens.append((i, token))
        return tokens

class TrigramTokenizer(Tokenizer):
    @log
    def tokenize(self, title, content = ''):
        document = title + content
        tokens = []
        for i in range(0, len(document)):
            token = document[i:i+3]
            if len(token) == 3 and self.stopwords.search(token) == None:
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
    def __init__(self, database_file, memory_mode, tokenizer_type):
        self._database_file = database_file
        self._memory_mode = memory_mode
        self._tokenizer = TokenizerFactory().create_tokenizer(tokenizer_type)
        self._inverted_index = {}
        self._connection = connect(self._database_file if not self._memory_mode else ':memory:', isolation_level = 'DEFERRED')
        cursor = self._connection.cursor()
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
                , content BLOB
            )
        """)
        self._connection.execute("PRAGMA journal_mode = OFF")
        self._connection.execute("PRAGMA synchronous = OFF")

    @log
    def add_index(self, title, content, document_id = 0):
        if(document_id == 0):
            document_id = self._store_document(title, content)
        self._create_posting_list(document_id, title, content)
        self._flush_buffer()

    @log
    def _store_document(self, title, content):
        cursor = self._connection.cursor()
        compressed = compress(content.encode('utf-8'), _COMPRESS_LEVEL)
        cursor.execute('INSERT INTO documents (title, content) VALUES(?, ?)', (title, compressed))
        lastrowid = cursor.lastrowid
        return lastrowid

    @log
    def _create_posting_list(self, document_id, title, content):
        tokens_exist = {i: token for i, token in self._tokenizer.tokenize(title, content) if token in self._inverted_index}
        tokens_not_exist = {i: token for i, token in self._tokenizer.tokenize(title, content) if token not in self._inverted_index}
        for i, token in tokens_exist.items():
            inverted_index_hash = self._inverted_index[token]
            inverted_index_hash.add(document_id, i)
        cursor = self._connection.cursor()
        cursor.execute('SELECT posting_list FROM indices WHERE token IN("{0}")'.format('", "'.join(str(token) for token in tokens_not_exist.values())))
        rows = cursor.fetchall()
        for row in rows:
            unpickled = loads(row[0])
            self._inverted_index[unpickled.token] = unpickled
        for i, token in tokens_not_exist.items():
            if token in self._inverted_index:
                inverted_index_hash = self._inverted_index[token]
                inverted_index_hash.add(document_id, i)
            else:
                self._inverted_index[token] = InvertedIndexHash(token, document_id, i)

    @log
    def _flush_buffer(self, final = False):
        total_number_positions = sum([item.positions_count for token, item in self._inverted_index.items()])
        if final or total_number_positions > _TOKEN_POSITION_LIMIT:
            cursor = self._connection.cursor()
            cursor.executemany('INSERT OR REPLACE INTO indices (token, posting_list) VALUES (?, ?)', [(k, dumps(v)) for k, v in self._inverted_index.items()])
            self._inverted_index = None
            del self._inverted_index
            gc.collect()
            self._inverted_index = {}

    @log
    def flush_memory_to_file(self):
        self._connection.commit()
        cursor = self._connection.cursor()
        cursor.execute("attach '{0}' as __extdb".format(self._database_file))
        cursor.execute("select name from sqlite_master where type='table'")
        table_names = cursor.fetchall()
        for table_name, in table_names:
            cursor.execute("create table __extdb.{0} as select * from {1}".format(table_name, table_name))
        cursor.execute("detach __extdb")

    @log
    def close_database_connection(self):
        self._flush_buffer(True)
        self._connection.commit()
        self._connection.close()

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
            self.posting_list[document_id].append(position)
            self.positions_count = self.positions_count + 1
        else:
            self.posting_list[document_id] = [position]
            self.positions_count = self.positions_count + 1

class Searcher(object):
    @log
    def __init__(self, database_file, memory_mode, tokenizer_type):
        self._database_file = database_file
        self._memory_mode = memory_mode
        self._tokenizer = TokenizerFactory().create_tokenizer(tokenizer_type)

    @log
    def search(self, words):
        matched_document_ids = None
        for word in split('\s+', words.strip(' 　')):
            tokens = self._tokenizer.tokenize(word)
            connection = connect(self._database_file, isolation_level = 'DEFERRED')
            connection.execute("PRAGMA journal_mode = OFF")
            connection.execute("PRAGMA synchronous = OFF")
            documents = {}
            with connection:
                cursor = connection.cursor()
                cursor.execute('SELECT posting_list FROM indices WHERE token IN("{0}")'.format('", "'.join(str(token) for i, token in tokens)))
                rows = cursor.fetchall()
                if len(rows) > 0:
                    for row in rows:
                        unpickled = loads(row[0])
                        for document_id, pl_item in unpickled.posting_list.items():
                            if document_id not in documents:
                                documents[document_id] = []
                            for position in pl_item:
                                documents[document_id].append((position, unpickled.token))
                else:
                    return None
            if matched_document_ids != None:
                matched_document_ids = self._get_matched_document_ids(documents, tokens, matched_document_ids)
            else:
                matched_document_ids = self._get_matched_document_ids(documents, tokens)
        documents = self._get_documents(matched_document_ids)
        connection.commit()
        return documents

    @log
    def _get_matched_document_ids(self, documents, tokens, prev_matched_document_ids = None):
        matched_document_ids = []
        number_of_tokens = len(tokens)
        for document_id, positions in documents.items():
            if prev_matched_document_ids != None and document_id not in prev_matched_document_ids:
                continue
            if number_of_tokens != len({token for pos, token in positions}):
                continue
            sequence = 1
            prev_position = -1
            for position, token in sorted(positions):
                if position - prev_position != 1:
                    sequence = 1
                if sequence == 1 or (sequence <= number_of_tokens and position - prev_position == 1):
                    if token == tokens[sequence-1][1]:
                        if number_of_tokens == sequence:
                            matched_document_ids.append(document_id)
                            continue
                        sequence = sequence + 1
                prev_position = position
        return matched_document_ids

    @log
    def _get_documents(self, matched_document_ids, return_content = False):
        if len(matched_document_ids) == 0:
            return []
        connection = connect(self._database_file)
        with connection:
            cursor = connection.cursor()
            if return_content:
                cursor.execute('SELECT id, title, content FROM documents WHERE id IN({0})'.format(', '.join(str(i) for i in matched_document_ids)))
                return [[id, title, str(decompress(content), encoding = 'utf-8')] for id, title, content in cursor.fetchall()]
            else:
                cursor.execute('SELECT id, title FROM documents WHERE id IN({0})'.format(', '.join(str(i) for i in matched_document_ids)))
                return [[id, title] for id, title in cursor.fetchall()]

class FalconHTTPRequestHandler(BaseHTTPRequestHandler):
    def initialize(self, database_file, tokenizer):
        self._database_file = database_file
        self._tokenizer = tokenizer

    def do_GET(self):
        url = urlparse(self.path)
        query_string = parse_qs(url.query)
        status_code = 200
        content_type = 'text/html'
        response_body = ''
        try:
            if url.path == '/search':
                if 'w' in query_string:
                    content_type = 'application/json'
                    searcher = Searcher(self._database_file, False, self._tokenizer)
                    search_results = searcher.search(query_string['w'][0])
                    response_body = json.dumps(search_results if search_results != None else [], ensure_ascii=False)
                else:
                    status_code = 400
                    response_body = 'Please enter search word(s).'
            elif url.path == '/add':
                if 't' in query_string and 'c' in query_string:
                    indexer = Indexer(self._database_file, False, self._tokenizer)
                    indexer.add_index(query_string['t'][0], query_string['c'][0])
                    indexer.close_database_connection()
                    response_body = 'Added:' + query_string['t'][0] + ' ' + query_string['c'][0]
                else:
                    status_code = 400
                    response_body = 'Please enter document title and content.'
            else:
                status_code = 404
                response_body = "Ooops, this page doesn't exist."
        except:
            status_code = 500
            response_body = 'Server error occured.'
        self.send_response(status_code)
        self.send_header('Content-type', content_type + ';charset=utf-8')
        self.end_headers()
        self.wfile.write(response_body.encode('utf-8'))
        return

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
        o = Searcher("", False, "Bigram")
        self.assertEqual(o._get_matched_document_ids({1 : [(0, 'bc'), (1, 'cd')], 2 : [(0, 'bc')]}, [(0, 'bc'), (1, 'cd')]), [1])
        self.assertEqual(o._get_matched_document_ids({1 : [(0, 'bc')], 2 : [(0, 'bc'), (1, 'cd')]}, [(0, 'bc'), (1, 'cd')]), [2])
        self.assertEqual(o._get_matched_document_ids({1 : [(0, 'bc'), (1, 'cd')], 2 : [(0, 'bc'), (1, 'cd')]}, [(0, 'bc'), (1, 'cd')]), [1, 2])
        self.assertEqual(o._get_matched_document_ids({1 : [(0, 'bc'), (1, 'cd'), (2, 'bc')], 2 : [(0, 'bc'), (1, 'cd'), (2, 'cd')]}, [(0, 'bc'), (1, 'cd')]), [1, 2])

class IndexManager(object):
    
    debug = False
    test_classes = (SearcherTest, TokenizerFactoryTest, BigramTokenizerTest, TrigramTokenizerTest)

    @log
    def run(self):
        parser = ArgumentParser(description='Falcon Full Text Search Engine')
        parser.add_argument('-C', '--showdocument', help='show document(s)', action='store_true')
        parser.add_argument('-D', '--debug', help='enable debug mode', action='store_true')
        parser.add_argument('-H', '--httpserver', help='run http server mode', action='store_true')
        parser.add_argument('-I', '--showindex', help='show index', action='store_true')
        parser.add_argument('-M', '--memorymode', help='enable in memory database mode', action='store_true')
        parser.add_argument('-T', '--test', help='run test', action='store_true')
        parser.add_argument('-c', '--content', metavar='content', help='document content to be stored and indexed')
        parser.add_argument('-d', '--databasefile', metavar='databasefile', help='a database file')
        parser.add_argument('-p', '--port', metavar='port', help='http port')
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

        if self._args.tokenizer == None:
            self._args.tokenizer = _DEFAULT_TOKENIZER

        if self._args.httpserver and self._args.databasefile != None:
            if self._args.port == None:
                self._args.port = _DEFAULT_PORT
            handler = FalconHTTPRequestHandler
            handler.initialize(handler, self._args.databasefile, self._args.tokenizer)
            httpd = HTTPServer(("", int(self._args.port)), handler)
            print("Falcon is serving at port", self._args.port)
            httpd.serve_forever()

        elif not self._args.httpserver and self._args.databasefile != None:
            if self._args.query != None:
                searcher = Searcher(self._args.databasefile, self._args.memorymode, self._args.tokenizer)
                search_results = searcher.search(self._args.query)
                if search_results != None:
                    for row in search_results:
                        print(row[0], row[1])
            elif self._args.title != None and self._args.content != None:
                indexer = Indexer(self._args.databasefile, self._args.memorymode, self._args.tokenizer)
                indexer.add_index(self._args.title, self._args.content)
            elif self._args.files != None:
                indexer = Indexer(self._args.databasefile, self._args.memorymode, self._args.tokenizer)
                for file_name in self._args.files:
                    with open(file_name) as f:
                        for line in f:
                            l = split('\s+', line, 1)
                            indexer.add_index(l[0], l[1])
                    if self._args.memorymode:
                        indexer.flush_memory_to_file()
                    else:
                        indexer.close_database_connection()

            if self._args.showindex:
                connection = connect(self._args.databasefile)
                with connection:
                    cursor = connection.cursor()
                    cursor.execute('SELECT token, posting_list FROM indices ORDER BY token')
                    for k1, v1 in cursor.fetchall():
                        o = loads(v1)
                        print(o.token, o.positions_count, o.posting_list)

            if self._args.showdocument:
                connection = connect(self._args.databasefile)
                with connection:
                    cursor = connection.cursor()
                    cursor.execute('SELECT id, title, content FROM documents')
                    for row in cursor.fetchall():
                        print(row[0], row[1], row[2])

if __name__ == '__main__':
    index_manager = IndexManager()
    index_manager.run()
