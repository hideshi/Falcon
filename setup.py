from distutils.core import setup

setup(
      name = 'Falcon'
    , py_modules = ['falcon']
    , scripts = ['falcon.py']
    , version = '0.1.0'
    , license = 'LGPL'
    , platforms = ['MacOS', 'POSIX']
    , description = 'Light Weight Full Text Search Engine'
    , author = 'hideshi'
    , author_email = 'hideshi.ogoshi@gmail.com'
    , url = 'https://github.com/hideshi/Falcon'
    , keywords = ['full', 'text', 'search', 'engine']
    , classifiers = [
          'License :: OSI Approved :: GNU Library or Lesser General Public License (LGPL)'
        , 'Operating System :: MacOS :: MacOS X'
        , 'Operating System :: POSIX :: Linux'
        , 'Programming Language :: Python'
        , 'Development Status :: 4 - Beta'
        , 'Environment :: Console'
        , 'Intended Audience :: Developers'
        , 'Topic :: Database'
        , 'Topic :: Database :: Database Engines/Servers'
        , 'Topic :: Text Processing :: Indexing'
        ]
    , long_description = '''\
Falcon Full Text Search Engine

Requirements
------------
* Python 3 or above

Features
--------
* Under construction

Setup
-----
::

   $ pip install Falcon

   History
   -------
   0.1.0 (2015-03-21)
   ~~~~~~~~~~~~~~~~~~
   * first release

Example
-------

.. code-block:: bash

    $ falcon.py -d test.db -z Bigram -t "Sample title" -c "Sample content"

'''
)
