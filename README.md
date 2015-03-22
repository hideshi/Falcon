Falcon Full Text Search Engine  
==============================
Falcon is a full text search engine using Python and SQLite3.  
It requires Python 3 or above. 
  
```
usage: falcon.py [-h] [-D] [-T] [-I] [-C] [-M] [-H] [-c content]
                 [-d databasefile] [-q query] [-t title] [-z tokenizer]
                 [files [files ...]]

Falcon Full Text Search Engine

positional arguments:
  files                 input file(s)

optional arguments:
  -h, --help            show this help message and exit
  -D, --debug           enable debug mode
  -T, --test            run test
  -I, --showindex       show index
  -C, --showdocument    show document(s)
  -M, --memorymode      enable in memory database mode
  -H, --httpserver      run http server mode
  -c content, --content content
                        document content to be stored and indexed
  -d databasefile, --databasefile databasefile
                        a sqlite3 database file
  -q query, --query query
                        query string
  -t title, --title title
                        document title to be stored and indexed
  -z tokenizer, --tokenizer tokenizer
                        Type of tokenizer [Bigram, Trigram]
```
