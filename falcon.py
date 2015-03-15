import sqlite3
import pickle
import re
import argparse

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
        self.stopwords = re.compile(r'[\s,.!\?"\'\$%&#:;\{\}\[\]\(\)、。]')

    @log
    def tokenize(self, title, content):
        raise NotImplementedError("tokenize method must be overridden and implemented by a descendant class.")

class BigramTokenizer(Tokenizer):
    def tokenize(self, title, content = None):
        '''
        >>> BigramTokenizer().tokenize('abcd')
        [(0, 'ab'), (1, 'bc'), (2, 'cd')]
        >>> BigramTokenizer().tokenize('a cd')
        [(2, 'cd')]
        '''
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
    def tokenize(self, title, content = None):
        '''
        >>> TrigramTokenizer().tokenize('abcde')
        [(0, 'abc'), (1, 'bcd'), (2, 'cde')]
        >>> TrigramTokenizer().tokenize('ab def')
        [(3, 'def')]
        '''
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
    def create_tokenizer(self, tokenizer):
        '''
        >>> TokenizerFactory().create_tokenizer("Bigram") #doctest: +ELLIPSIS
        <falcon.BigramTokenizer object at 0x...>
        >>> TokenizerFactory().create_tokenizer("Trigram") #doctest: +ELLIPSIS
        <falcon.TrigramTokenizer object at 0x...>
        '''
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
    def delete_index(self):
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
        matched_document_ids = []
        print(tokens)
        for i, token in tokens:
            with connection:
                cursor = connection.cursor()
                cursor.execute('SELECT token, posting_list FROM indexes WHERE token = ? ORDER BY token ASC', (token,))
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
        print(documents)
        for document_id, positions in documents.items():
            print('document_id:', document_id)
            sorted_positions = sorted(positions)
            number_of_tokens = len(tokens)
            sequence = 0
            prev_position = -1
            for position, token in sorted_positions:
                print(position, token)
                print('sequence:', sequence)
                print('prev_position:', prev_position)
                if position - prev_position != 1:
                    sequence = 0
                if sequence == 0 or position - prev_position == 1:
                    print('token:', token)
                    print('token2:', tokens[sequence][1])
                    if token == tokens[sequence][1]:
                        sequence = sequence + 1
                        if number_of_tokens == sequence:
                            print('matched')
                            matched_document_ids.append(document_id)
                prev_position = position
        return matched_document_ids

class IndexManager(object):
    
    debug = False

    @log
    def run(self):
        parser = argparse.ArgumentParser(description='Falcon Full Text Search Engine')
        parser.add_argument('-D', '--debug', help='enable debug mode', action='store_true')
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

        if self._args.databasefile != None:
            if self._args.query != None:
                if self._args.tokenizer != None:
                    searcher = Searcher(self._args.databasefile, self._args.tokenizer)
                else:
                    searcher = Searcher(self._args.databasefile)
                search_results = searcher.search(self._args.query)
                for k in search_results:
                    print(k)
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
                    cursor.execute('SELECT token, posting_list FROM indexes ORDER BY token')
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
    from doctest import testmod
    testmod()
    index_manager = IndexManager()
    index_manager.run()
