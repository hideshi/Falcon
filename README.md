Falcon Full Text Search Engine  
==============================
Falcon is a full text search engine using Python and SQLite3.  
It requires Python 3 or above. 
  
```
usage: falcon.py [-h] [-C] [-D] [-H] [-I] [-M] [-T] [-c content]
                 [-d databasefile] [-p port] [-q query] [-t title]
                 [-z tokenizer]
                 [files [files ...]]

Falcon Full Text Search Engine

positional arguments:
  files                 input file(s)

optional arguments:
  -h, --help            show this help message and exit
  -C, --showdocument    show document(s)
  -D, --debug           enable debug mode
  -H, --httpserver      run http server mode
  -I, --showindex       show index
  -M, --memorymode      enable in memory database mode
  -T, --test            run test
  -c content, --content content
                        document content to be stored and indexed
  -d databasefile, --databasefile databasefile
                        a sqlite3 database file
  -p port, --port port  http port
  -q query, --query query
                        query string
  -t title, --title title
                        document title to be stored and indexed
  -z tokenizer, --tokenizer tokenizer
                        Type of tokenizer [Bigram, Trigram]
```

```
# run http server mode
# default port number = 8888
$ falcon.py -d database_file -H -p 8080

# search
# accept multiple search word divided by spaces
http://hostname:8080/search?w=search_word

# add index
http://hostname:8080/add?t=title&c=content
```
