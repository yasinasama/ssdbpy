from __future__ import with_statement
from itertools import chain
import datetime
import sys
import warnings
import time
import threading
import time as mod_time
import hashlib
from ssdb._compat import (b, basestring, bytes, imap, iteritems, iterkeys,
                           itervalues, izip, long, nativestr, unicode,
                           safe_unicode)
from ssdb.connection import (ConnectionPool, UnixDomainSocketConnection,
                              SSLConnection, Token)
from ssdb.lock import Lock, LuaLock
from ssdb.exceptions import (
    ConnectionError,
    DataError,
    ExecAbortError,
    NoScriptError,
    PubSubError,
    RedisError,
    ResponseError,
    TimeoutError,
    WatchError,
)

SYM_EMPTY = b('')


def list_or_args(keys, args):
    # returns a single list combining keys and args
    try:
        iter(keys)
        # a string or bytes instance can be iterated, but indicates
        # keys wasn't passed as a list
        if isinstance(keys, (basestring, bytes)):
            keys = [keys]
    except TypeError:
        keys = [keys]
    if args:
        keys.extend(args)
    return keys


def timestamp_to_datetime(response):
    "Converts a unix timestamp to a Python datetime object"
    if not response:
        return None
    try:
        response = int(response)
    except ValueError:
        return None
    return datetime.datetime.fromtimestamp(response)


def string_keys_to_dict(key_string, callback):
    return dict.fromkeys(key_string.split(), callback)


def dict_merge(*dicts):
    merged = {}
    for d in dicts:
        merged.update(d)
    return merged


def parse_info(response):
    "Parse the result of Redis's INFO command into a Python dict"
    info = {}
    response = response[1:]
    for index,value in enumerate(response[:8:2]):
        info[value] = response[index*2+1]
    return info

def pairs_to_dict(response):
    "Create a dict given a list of key/value pairs"
    it = iter(response)
    return dict(izip(it, it))


def pairs_to_dict_typed(response, type_info):
    it = iter(response)
    result = {}
    for key, value in izip(it, it):
        if key in type_info:
            try:
                value = type_info[key](value)
            except:
                # if for some reason the value can't be coerced, just use
                # the string value
                pass
        result[key] = value
    return result


def parse_scan(response):
    result = {}
    for index,key in enumerate(response[::2]):
        value = response[index*2+1]
        result.setdefault(key,value)
    return result


class SSDB(object):
    """
    Implementation of the ssdb protocol.

    This abstract class provides a Python interface to all ssdb commands
    and an implementation of the ssdb protocol.

    Connection and Pipeline derive from this, implementing how
    the commands are sent and received to the ssdb server
    """
    RESPONSE_CALLBACKS = dict_merge(
        string_keys_to_dict(
            'auth exists hexists zexists',
            lambda r: bool(int(r[0]))
        ),
        string_keys_to_dict(
            'set setx setnx expire ttl del incr getbit setbit bitcount '
            'countbit strlen multi_set multi_del hset hdel hincr hsize '
            'hclear multi_hset multi_hdel zset zdel zincr zsize zclear '
            'zcount zsum zremrangebyrank zremrangebyscore multi_zset multi_zdel '
            'qpush_front qpush_back qpush qsize qclear qtrim_front qtrim_back',
            lambda r: int(r[0])
        ),
        string_keys_to_dict(
            'scan rscan multi_get hgetall hrscan multi_hget zscan zrscan '
            'zrange zrrange zpop_front zpop_back multi_zget',
            parse_scan
        ),
        string_keys_to_dict(
            'get getset hget zget zrank zrrank qfront qback qget',
            lambda r: r[0] if len(r) > 0 else None
        ),
        {
            'zavg': lambda r: float(r[0]),
            'info': parse_info,
            'substr': lambda r: r[0]
        }
    )

    @classmethod
    def from_url(cls, url, db=None, **kwargs):
        """
        Return a Redis client object configured from the given URL, which must
        use either `the ``redis://`` scheme
        <http://www.iana.org/assignments/uri-schemes/prov/redis>`_ for RESP
        connections or the ``unix://`` scheme for Unix domain sockets.

        For example::

            redis://[:password]@localhost:6379/0
            unix://[:password]@/path/to/socket.sock?db=0

        There are several ways to specify a database number. The parse function
        will return the first specified option:
            1. A ``db`` querystring option, e.g. redis://localhost?db=0
            2. If using the redis:// scheme, the path argument of the url, e.g.
               redis://localhost/0
            3. The ``db`` argument to this function.

        If none of these options are specified, db=0 is used.

        Any additional querystring arguments and keyword arguments will be
        passed along to the ConnectionPool class's initializer. In the case
        of conflicting arguments, querystring arguments always win.
        """
        connection_pool = ConnectionPool.from_url(url, db=db, **kwargs)
        return cls(connection_pool=connection_pool)

    def __init__(self, host='localhost', port=6379,
                 db=0, password=None, socket_timeout=None,
                 socket_connect_timeout=None,
                 socket_keepalive=None, socket_keepalive_options=None,
                 connection_pool=None, unix_socket_path=None,
                 encoding='utf-8', encoding_errors='strict',
                 charset=None, errors=None,
                 decode_responses=False, retry_on_timeout=False,
                 ssl=False, ssl_keyfile=None, ssl_certfile=None,
                 ssl_cert_reqs=None, ssl_ca_certs=None,
                 max_connections=None):
        if not connection_pool:
            if charset is not None:
                warnings.warn(DeprecationWarning(
                    '"charset" is deprecated. Use "encoding" instead'))
                encoding = charset
            if errors is not None:
                warnings.warn(DeprecationWarning(
                    '"errors" is deprecated. Use "encoding_errors" instead'))
                encoding_errors = errors

            kwargs = {
                'db': db,
                'password': password,
                'socket_timeout': socket_timeout,
                'encoding': encoding,
                'encoding_errors': encoding_errors,
                'decode_responses': decode_responses,
                'retry_on_timeout': retry_on_timeout,
                'max_connections': max_connections
            }
            # based on input, setup appropriate connection args
            if unix_socket_path is not None:
                kwargs.update({
                    'path': unix_socket_path,
                    'connection_class': UnixDomainSocketConnection
                })
            else:
                # TCP specific options
                kwargs.update({
                    'host': host,
                    'port': port,
                    'socket_connect_timeout': socket_connect_timeout,
                    'socket_keepalive': socket_keepalive,
                    'socket_keepalive_options': socket_keepalive_options,
                })

                if ssl:
                    kwargs.update({
                        'connection_class': SSLConnection,
                        'ssl_keyfile': ssl_keyfile,
                        'ssl_certfile': ssl_certfile,
                        'ssl_cert_reqs': ssl_cert_reqs,
                        'ssl_ca_certs': ssl_ca_certs,
                    })
            connection_pool = ConnectionPool(**kwargs)
        self.connection_pool = connection_pool
        self._use_lua_lock = None

        self.response_callbacks = self.__class__.RESPONSE_CALLBACKS.copy()

    def __repr__(self):
        return "%s<%s>" % (type(self).__name__, repr(self.connection_pool))

    def set_response_callback(self, command, callback):
        "Set a custom Response Callback"
        self.response_callbacks[command] = callback


    def transaction(self, func, *watches, **kwargs):
        """
        Convenience method for executing the callable `func` as a transaction
        while watching all keys specified in `watches`. The 'func' callable
        should expect a single argument which is a Pipeline object.
        """
        shard_hint = kwargs.pop('shard_hint', None)
        value_from_callable = kwargs.pop('value_from_callable', False)
        watch_delay = kwargs.pop('watch_delay', None)
        with self.pipeline(True, shard_hint) as pipe:
            while 1:
                try:
                    if watches:
                        pipe.watch(*watches)
                    func_value = func(pipe)
                    exec_value = pipe.execute()
                    return func_value if value_from_callable else exec_value
                except WatchError:
                    if watch_delay is not None and watch_delay > 0:
                        time.sleep(watch_delay)
                    continue

    def lock(self, name, timeout=None, sleep=0.1, blocking_timeout=None,
             lock_class=None, thread_local=True):
        """
        Return a new Lock object using key ``name`` that mimics
        the behavior of threading.Lock.

        If specified, ``timeout`` indicates a maximum life for the lock.
        By default, it will remain locked until release() is called.

        ``sleep`` indicates the amount of time to sleep per loop iteration
        when the lock is in blocking mode and another client is currently
        holding the lock.

        ``blocking_timeout`` indicates the maximum amount of time in seconds to
        spend trying to acquire the lock. A value of ``None`` indicates
        continue trying forever. ``blocking_timeout`` can be specified as a
        float or integer, both representing the number of seconds to wait.

        ``lock_class`` forces the specified lock implementation.

        ``thread_local`` indicates whether the lock token is placed in
        thread-local storage. By default, the token is placed in thread local
        storage so that a thread only sees its token, not a token set by
        another thread. Consider the following timeline:

            time: 0, thread-1 acquires `my-lock`, with a timeout of 5 seconds.
                     thread-1 sets the token to "abc"
            time: 1, thread-2 blocks trying to acquire `my-lock` using the
                     Lock instance.
            time: 5, thread-1 has not yet completed. redis expires the lock
                     key.
            time: 5, thread-2 acquired `my-lock` now that it's available.
                     thread-2 sets the token to "xyz"
            time: 6, thread-1 finishes its work and calls release(). if the
                     token is *not* stored in thread local storage, then
                     thread-1 would see the token value as "xyz" and would be
                     able to successfully release the thread-2's lock.

        In some use cases it's necessary to disable thread local storage. For
        example, if you have code where one thread acquires a lock and passes
        that lock instance to a worker thread to release later. If thread
        local storage isn't disabled in this case, the worker thread won't see
        the token set by the thread that acquired the lock. Our assumption
        is that these cases aren't common and as such default to using
        thread local storage.        """
        if lock_class is None:
            if self._use_lua_lock is None:
                # the first time .lock() is called, determine if we can use
                # Lua by attempting to register the necessary scripts
                try:
                    LuaLock.register_scripts(self)
                    self._use_lua_lock = True
                except ResponseError:
                    self._use_lua_lock = False
            lock_class = self._use_lua_lock and LuaLock or Lock
        return lock_class(self, name, timeout=timeout, sleep=sleep,
                          blocking_timeout=blocking_timeout,
                          thread_local=thread_local)


    # COMMAND EXECUTION AND PROTOCOL PARSING
    def execute_command(self, *args, **options):
        "Execute a command and return a parsed response"
        pool = self.connection_pool
        command_name = args[0]
        connection = pool.get_connection(command_name, **options)
        try:
            connection.send_command(*args)
            return self.parse_response(connection, command_name, **options)
        except (ConnectionError, TimeoutError) as e:
            connection.disconnect()
            if not connection.retry_on_timeout and isinstance(e, TimeoutError):
                raise
            connection.send_command(*args)
            return self.parse_response(connection, command_name, **options)
        finally:
            pool.release(connection)

    def parse_response(self, connection, command_name, **options):
        "Parses a response from the Redis server"
        response = connection.read_response()

        # status_code = response[0]
        # if status_code == 'ok'
        content = response[1:]
        if command_name in self.response_callbacks:
            return self.response_callbacks[command_name](content, **options)
        return content

    def auth(self,password):
        return self.execute_command('auth',password)


    def dbsize(self):
        "Returns the memory usage of the current database"
        return self.execute_command('dbsize')


    def flushdb(self):
        "Delete all keys in all databases on the current host"
        return self.execute_command('flushdb')


    def info(self, section=None):
        """
        Returns a dictionary containing information about the Redis server

        The ``section`` option can be used to select a specific section
        of information

        The section option is not supported by older versions of Redis Server,
        and will generate ResponseError
        """
        if section is None:
            return self.execute_command('info')
        else:
            return self.execute_command('info', section)


    def bitcount(self, key, start=None, end=None):
        """
        Returns the count of set bits in the value of ``key``.  Optional
        ``start`` and ``end`` paramaters indicate which bytes to consider
        """
        params = [key]

        if start is not None:
            params.append(start)

        if end is not None:
            params.append(end)

        return self.execute_command('bitcount', *params)

    def countbit(self, key, start=None, size=None):
        """
        Returns the count of set bits in the value of ``key``.  Optional
        ``start`` and ``size`` paramaters indicate which bytes to consider
        """
        params = [key]

        if start is not None:
            params.append(start)

        if size is not None:
            params.append(size)

        return self.execute_command('countbit', *params)


    def delete(self, *names):
        "Delete one or more keys specified by ``names``"
        return self.execute_command('del', *names)

    def __delitem__(self, name):
        self.delete(name)


    def exists(self, name):
        "Returns a boolean indicating whether key ``name`` exists"
        return self.execute_command('exists', name)
    __contains__ = exists

    def expire(self, name, time):
        """
        Set an expire flag on key ``name`` for ``time`` seconds.
        """
        return self.execute_command('expire', name, time)


    def get(self, name):
        """
        Return the value at key ``name``, or None if the key doesn't exist
        """
        return self.execute_command('get', name)

    def __getitem__(self, name):
        """
        Return the value at key ``name``, raises a KeyError if the key
        doesn't exist.
        """
        value = self.get(name)
        if value is not None:
            return value
        raise KeyError(name)

    def getbit(self, name, offset):
        "Returns a boolean indicating the value of ``offset`` in ``name``"
        return self.execute_command('getbit', name, offset)


    def getset(self, name, value):
        """
        Sets the value at key ``name`` to ``value``
        and returns the old value at key ``name`` atomically.
        """
        return self.execute_command('getset', name, value)

    def incr(self, name, amount=1):
        """
        Increments the value of ``key`` by ``amount``.  If no key exists,
        the value will be initialized as ``amount``
        """
        return self.execute_command('incr', name, amount)


    def keys(self, start='',end='',limit=-1):
        "Returns a list of keys matching ``pattern``"

        return self.execute_command('keys', start,end,limit)

    def rkeys(self, start='',end='',limit=-1):
        "Returns a list of reverse keys matching ``pattern``"

        return self.execute_command('rkeys', start,end,limit)

    def hlist(self, start='',end='',limit=-1):
        "Returns a list of keys matching ``pattern``"

        return self.execute_command('hlist', start,end,limit)

    def hrlist(self, start='',end='',limit=-1):
        "Returns a list of keys matching ``pattern``"

        return self.execute_command('hrlist', start,end,limit)

    def hkeys(self,name, start='',end='',limit=-1):
        "Returns a list of keys matching ``pattern``"

        return self.execute_command('hkeys',name, start,end,limit)

    def set(self,name,value):
        """
        Set the value at key ``name`` to ``value``
        """
        return self.execute_command('set', name,value)

    def setx(self, name, value, ttl):
        """
        Set the value at key ``name`` to ``value``

        ``ttl`` sets an expire flag on key ``name``(s).

        """
        return self.execute_command('setx', name, value, ttl)

    def __setitem__(self, name, value):
        self.set(name, value)

    def setbit(self, name, offset, value):
        """
        Flag the ``offset`` in ``name`` as ``value``. Returns a boolean
        indicating the previous value of ``offset``.
        """
        value = value and 1 or 0
        return self.execute_command('setbit', name, offset, value)

    def setnx(self, name, value):
        "Set the value of key ``name`` to ``value`` if key doesn't exist"
        return self.execute_command('setnx', name, value)

    def strlen(self, name):
        "Return the number of bytes stored in the value of ``name``"
        return self.execute_command('strlen', name)

    def substr(self, name, start=None, size=None):
        """
        Return a substring of the string at key ``name``. ``start`` and ``end``
        are 0-based integers specifying the portion of the string to return.
        """
        params = [name]

        if start is not None:
            params.append(start)

        if size is not None:
            params.append(size)

        return self.execute_command('substr',*params)


    def ttl(self, name):
        "Returns the number of seconds until the key ``name`` will expire"
        return self.execute_command('ttl', name)


    # SCAN COMMANDS
    def scan(self, start='', end='', limit=-1):
        """
        Incrementally return lists of key value. Also return a cursor
        indicating the scan position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        return self.execute_command('scan', start,end,limit)

    def hscan(self,name, start='', end='', limit=-1):
        """
        Incrementally return lists of key value. Also return a cursor
        indicating the scan position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        return self.execute_command('hscan',name, start,end,limit)

    def hrscan(self,name, start='', end='', limit=-1):
        """
        Incrementally return lists of key value. Also return a cursor
        indicating the scan position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        return self.execute_command('hrscan',name, start,end,limit)

    def hclear(self,name):
        """
        Incrementally return lists of key value. Also return a cursor
        indicating the scan position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        return self.execute_command('hclear',name)

    def rscan(self, start='', end='', limit=-1):
        """
        Incrementally return lists of key names. Also return a cursor
        indicating the scan position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        return self.execute_command('rscan', start,end,limit)

    def multi_set(self, kvs):
        """
        Incrementally return lists of key names. Also return a cursor
        indicating the scan position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        params = []
        for key,value in kvs.items():
            params.append(key)
            params.append(value)
        return self.execute_command('multi_set', *params)

    def multi_hset(self,name, hkvs):
        """
        Incrementally return lists of key names. Also return a cursor
        indicating the scan position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        params = []
        for key,value in hkvs.items():
            params.append(key)
            params.append(value)
        return self.execute_command('multi_hset',name, *params)

    def multi_get(self, keys):
        """
        Incrementally return lists of key names. Also return a cursor
        indicating the scan position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        return self.execute_command('multi_get', *keys)

    def multi_hget(self,name, keys):
        """
        Incrementally return lists of key names. Also return a cursor
        indicating the scan position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        return self.execute_command('multi_hget',name, *keys)

    def multi_del(self, keys):
        """
        Incrementally return lists of key names. Also return a cursor
        indicating the scan position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        return self.execute_command('multi_del', *keys)

    def multi_hdel(self,name, keys):
        """
        Incrementally return lists of key names. Also return a cursor
        indicating the scan position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        return self.execute_command('multi_hdel',name, *keys)

    def scan_iter(self, match=None, count=None):
        """
        Make an iterator using the SCAN command so that the client doesn't
        need to remember the cursor position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        cursor = '0'
        while cursor != 0:
            cursor, data = self.scan(cursor=cursor, match=match, count=count)
            for item in data:
                yield item

    def zscan_iter(self, name, match=None, count=None,
                   score_cast_func=float):
        """
        Make an iterator using the ZSCAN command so that the client doesn't
        need to remember the cursor position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns

        ``score_cast_func`` a callable used to cast the score return value
        """
        cursor = '0'
        while cursor != 0:
            cursor, data = self.zscan(name, cursor=cursor, match=match,
                                      count=count,
                                      score_cast_func=score_cast_func)
            for item in data:
                yield item

    def zset(self, name, key, score):
        """
        Store the union of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.
        """
        return self.execute_command('zset', name, key,score)

    def zget(self, name, key):
        """
        Store the union of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.
        """
        return self.execute_command('zget', name, key)

    def zdel(self, name, key):
        """
        Store the union of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.
        """
        return self.execute_command('zdel', name, key)

    def zincr(self, name, key,num):
        """
        Store the union of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.
        """
        return self.execute_command('zincr', name, key,num)

    def zexists(self, name, key):
        """
        Store the union of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.
        """
        return self.execute_command('zexists', name, key)

    def zsize(self, name):
        """
        Store the union of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.
        """
        return self.execute_command('zsize', name)

    def zlist(self, start='',end='',limit=-1):
        "Returns a list of keys matching ``pattern``"

        return self.execute_command('zlist', start,end,limit)

    def zrlist(self, start='',end='',limit=-1):
        "Returns a list of keys matching ``pattern``"

        return self.execute_command('zrlist', start,end,limit)

    def zkeys(self,name,key_start, score_start='',score_end='',limit=-1):
        "Returns a list of keys matching ``pattern``"

        return self.execute_command('zkeys',name,key_start, score_start,score_end,limit)

    def zscan(self,name,key_start, score_start='',score_end='',limit=-1):
        "Returns a list of keys matching ``pattern``"

        return self.execute_command('zscan',name,key_start, score_start,score_end,limit)

    def zrscan(self,name,key_start, score_start='',score_end='',limit=-1):
        "Returns a list of keys matching ``pattern``"

        return self.execute_command('zrscan',name,key_start, score_start,score_end,limit)

    def zrank(self,name,key):
        "Returns a list of keys matching ``pattern``"

        return self.execute_command('zrank',name,key)

    def zrrank(self,name,key):
        "Returns a list of keys matching ``pattern``"

        return self.execute_command('zrrank',name,key)

    def zrange(self,name,offset,limit):
        "Returns a list of keys matching ``pattern``"

        return self.execute_command('zrange',name,offset,limit)

    def zrrange(self,name,offset,limit):
        "Returns a list of keys matching ``pattern``"

        return self.execute_command('zrrange',name,offset,limit)

    def zclear(self,name):
        "Returns a list of keys matching ``pattern``"

        return self.execute_command('zclear',name)

    def zcount(self,name,start,end):
        "Returns a list of keys matching ``pattern``"

        return self.execute_command('zcount',name,start,end)

    def zsum(self,name,start,end):
        "Returns a list of keys matching ``pattern``"

        return self.execute_command('zsum',name,start,end)

    def zavg(self,name,start,end):
        "Returns a list of keys matching ``pattern``"

        return self.execute_command('zavg',name,start,end)

    def zremrangebyrank(self,name,start,end):
        "Returns a list of keys matching ``pattern``"

        return self.execute_command('zremrangebyrank',name,start,end)

    def zremrangebyscore(self,name,start,end):
        "Returns a list of keys matching ``pattern``"

        return self.execute_command('zremrangebyscore',name,start,end)

    def zpop_front(self,name,limit):
        "Returns a list of keys matching ``pattern``"

        return self.execute_command('zpop_front',name,limit)

    def zpop_back(self,name,limit):
        "Returns a list of keys matching ``pattern``"

        return self.execute_command('zpop_back',name,limit)

    def multi_zset(self,name, zkvs):
        """
        Incrementally return lists of key names. Also return a cursor
        indicating the scan position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        params = []
        for key,value in zkvs.items():
            params.append(key)
            params.append(value)
        return self.execute_command('multi_zset',name, *params)

    def multi_zget(self,name, keys):
        """
        Incrementally return lists of key names. Also return a cursor
        indicating the scan position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        return self.execute_command('multi_zget',name, *keys)

    def multi_zdel(self,name, keys):
        """
        Incrementally return lists of key names. Also return a cursor
        indicating the scan position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        return self.execute_command('multi_zdel',name, *keys)

    def qpush_front(self,name,items):

        return self.execute_command('qpush_front',name,*items)

    def qpush_back(self,name,items):

        return self.execute_command('qpush_back',name,*items)

    def qpop_front(self,name,size):

        return self.execute_command('qpop_front',name,size)

    def qpop_back(self,name,size):

        return self.execute_command('qpop_back',name,size)

    qpush = qpush_back
    qpop = qpop_front

    def qfront(self,name):

        return self.execute_command('qfront',name)

    def qback(self,name):

        return self.execute_command('qback',name)

    def qsize(self,name):

        return self.execute_command('qsize',name)

    def qclear(self,name):

        return self.execute_command('qclear',name)

    def qget(self,name,index):

        return self.execute_command('qget',name,index)

    def qset(self,name,index,value):

        return self.execute_command('qset',name,index,value)

    def qrange(self,name,offset,limit):

        return self.execute_command('qrange',name,offset,limit)

    def qslice(self,name,begin,end):

        return self.execute_command('qslice',name,begin,end)

    def qtrim_front(self,name,size):

        return self.execute_command('qtrim_front',name,size)

    def qtrim_back(self,name,size):

        return self.execute_command('qtrim_back',name,size)

    def qlist(self,start,end,limit):

        return self.execute_command('qlist',start,end,limit)

    def qrlist(self,start,end,limit):

        return self.execute_command('qrlist',start,end,limit)

    def add_allow_ip(self,rule):

        return self.execute_command('add_allow_ip',rule)

    def list_allow_ip(self):

        return self.execute_command('list_allow_ip')



    # HASH COMMANDS
    def hdel(self, name, *keys):
        "Delete ``keys`` from hash ``name``"
        return self.execute_command('hdel', name, *keys)

    def hexists(self, name, key):
        "Returns a boolean indicating if ``key`` exists within hash ``name``"
        return self.execute_command('hexists', name, key)

    def hget(self, name, key):
        "Return the value of ``key`` within the hash ``name``"
        return self.execute_command('hget', name, key)

    def hgetall(self, name):
        "Return a Python dict of the hash's name/value pairs"
        return self.execute_command('hgetall', name)

    def hincr(self, name, key, amount=1):
        "Increment the value of ``key`` in hash ``name`` by ``amount``"
        return self.execute_command('hincr', name, key, amount)


    def hsize(self, name):
        "Return the number of elements in hash ``name``"
        return self.execute_command('hsize', name)

    def hset(self, name, key, value):
        """
        Set ``key`` to ``value`` within hash ``name``
        Returns 1 if HSET created a new field, otherwise 0
        """
        return self.execute_command('hset', name, key, value)
