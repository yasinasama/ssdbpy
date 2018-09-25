ssdbpy
======

The Python interface to the ssdb

Installation
------------

To install ssdb:

.. code-block:: bash

    $ sudo python setup.py install

Getting Started
---------------

.. code-block:: pycon

    >>> import ssdb
    >>> r = ssdb.SSDB(host='localhost',port=8888)
    >>> r.set('foo', 'bar')
    True
    >>> r.get('foo')
    'bar'

Connection Pools

.. code-block:: pycon

    >>> import ssdb
    >>> pool = ssdb.ConnectionPool(host='localhost',port=8888)
    >>> r = ssdb.SSDB(connection_pool=pool)