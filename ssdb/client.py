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
from ssdb.connection import (ConnectionPool,Token)
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


def string_keys_to_dict(key_string, callback):
    return dict.fromkeys(key_string.split(), callback)


def dict_merge(*dicts):
    merged = {}
    for d in dicts:
        merged.update(d)
    return merged


def response_info(response):
    response_iter = iter(response[1:])
    return dict(izip(response_iter, response_iter))

def response_to_bool(response):
    return bool(int(response[0]))

def response_to_int(response):
    return int(response[0])

def response_to_float(response):
    return float(response[0])

def response_to_dict(response):
    response_iter = iter(response)
    return dict(izip(response_iter,response_iter))

def response_to_value(response):
    return response[0]

def response_to_value_or_none(response):
    return response[0] if len(response) > 0 else None

class SSDB(object):
    """
    Implementation of the ssdb protocol.

    This abstract class provides a Python interface to all ssdb commands
    and an implementation of the ssdb protocol.
    """
    RESPONSE_CALLBACKS = dict_merge(
        string_keys_to_dict(
            'auth exists hexists zexists',
            response_to_bool
        ),
        string_keys_to_dict(
            'set setx setnx expire ttl del incr getbit setbit bitcount '
            'countbit strlen multi_set multi_del hset hdel hincr hsize '
            'hclear multi_hset multi_hdel zset zdel zincr zsize zclear '
            'zcount zsum zremrangebyrank zremrangebyscore multi_zset multi_zdel '
            'qpush_front qpush_back qpush qsize qclear qtrim_front qtrim_back',
            response_to_int
        ),
        string_keys_to_dict(
            'scan rscan multi_get hgetall hrscan multi_hget zscan zrscan '
            'zrange zrrange zpop_front zpop_back multi_zget',
            response_to_dict
        ),
        string_keys_to_dict(
            'get getset hget zget zrank zrrank qfront qback qget',
            response_to_value_or_none
        ),
        {
            'zavg': response_to_float,
            'info': response_info,
            'substr': response_to_value
        }
    )

    @classmethod
    def from_url(cls, url, **kwargs):
        """
        Return a SSDB client object configured from the given URL, which must
        use either `the ``ssdb://`` scheme

        For example::

            ssdb://[:password]@localhost:8888

        Any additional querystring arguments and keyword arguments will be
        passed along to the ConnectionPool class's initializer. In the case
        of conflicting arguments, querystring arguments always win.
        """
        connection_pool = ConnectionPool.from_url(url, **kwargs)
        return cls(connection_pool=connection_pool)

    def __init__(self, host='localhost', port=8888,
                 password=None, socket_timeout=None,
                 socket_connect_timeout=None,
                 socket_keepalive=None, socket_keepalive_options=None,
                 connection_pool=None,
                 encoding='utf-8', encoding_errors='strict',
                 charset=None, errors=None,
                 decode_responses=False, retry_on_timeout=False,
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
                'host': host,
                'port': port,
                'socket_connect_timeout': socket_connect_timeout,
                'socket_keepalive': socket_keepalive,
                'socket_keepalive_options': socket_keepalive_options,
                'password': password,
                'socket_timeout': socket_timeout,
                'encoding': encoding,
                'encoding_errors': encoding_errors,
                'decode_responses': decode_responses,
                'retry_on_timeout': retry_on_timeout,
                'max_connections': max_connections
            }

            connection_pool = ConnectionPool(**kwargs)
        self.connection_pool = connection_pool

        self.response_callbacks = self.__class__.RESPONSE_CALLBACKS.copy()

    def __repr__(self):
        return "%s<%s>" % (type(self).__name__, repr(self.connection_pool))

    def set_response_callback(self, command, callback):
        "Set a custom Response Callback"
        self.response_callbacks[command] = callback

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

    # key value command
    def set(self,name,value):
        """
        Set the value of the key
        """
        return self.execute_command('set', name,value)

    def __setitem__(self, name, value):
        self.set(name, value)

    def setx(self, name, value, ttl):
        """
        Set the value of the key, with a time to live
        """
        return self.execute_command('setx', name, value, ttl)

    def setnx(self, name, value):
        """
        Set the string value in argument as value of the key
        if and only if the key doesn't exist.
        """
        return self.execute_command('setnx', name, value)

    def expire(self, name, time):
        """
        Set the time left to live in seconds, only for keys of KV type.
        """
        return self.execute_command('expire', name, time)

    def ttl(self, name):
        """
        Returns the time left to live in seconds, only for keys of KV type.
        """
        return self.execute_command('ttl', name)

    def get(self, name):
        """
        Get the value related to the specified key.
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

    def getset(self, name, value):
        """
        Sets a value and returns the previous entry at that key.
        """
        return self.execute_command('getset', name, value)

    def delete(self, *names):
        """
        Delete specified key.
        """
        return self.execute_command('del', *names)

    def __delitem__(self, name):
        self.delete(name)

    def incr(self, name, amount):
        """
        Increments the value of ``key`` by ``amount``.  If no key exists,
        the value will be initialized as ``amount``
        """
        return self.execute_command('incr', name, amount)

    def exists(self, name):
        """
        Verify if the specified key exists.
        """
        return self.execute_command('exists', name)

    __contains__ = exists

    def getbit(self, name, offset):
        """
        Return a single bit out of a string.
        """
        return self.execute_command('getbit', name, offset)

    def setbit(self, name, offset, value):
        """
        Changes a single bit of a string. The string is auto expanded.
        """
        value = value and 1 or 0
        return self.execute_command('setbit', name, offset, value)

    def bitcount(self, key, start=None, end=None):
        """
        Count the number of set bits (population counting) in a string.
        Like Redis's bitcount.
        """
        params = [key]

        if start is not None:
            params.append(start)

        if end is not None:
            params.append(end)

        return self.execute_command('bitcount', *params)

    def countbit(self, key, start=None, size=None):
        """
        Count the number of set bits (population counting) in a string.
        Unlike bitcount, it take part of the string by start and size, not start and end.
        """
        params = [key]

        if start is not None:
            params.append(start)

        if size is not None:
            params.append(size)

        return self.execute_command('countbit', *params)

    def substr(self, name, start=None, size=None):
        """
        Return part of a string, like PHP's substr() function.
        """
        params = [name]

        if start is not None:
            params.append(start)

        if size is not None:
            params.append(size)

        return self.execute_command('substr',*params)

    def strlen(self, name):
        """
        Return the number of bytes of a string.
        """
        return self.execute_command('strlen', name)

    def keys(self, start='',end='',limit=-1):
        """
        Refer to scan command for more information about how it work.
        """
        return self.execute_command('keys', start,end,limit)

    def rkeys(self, start='',end='',limit=-1):
        """
        Like keys, but in reverse order.
        """
        return self.execute_command('rkeys', start,end,limit)

    def scan(self, start='', end='', limit=-1):
        """
        List key-value pairs with keys in range (start, end].
        ("", ""] means no range limit.
        This command can do wildchar * like search, but only prefix search,
        and the * char must never occur in start and end!
        """
        return self.execute_command('scan', start,end,limit)

    def rscan(self, start='', end='', limit=-1):
        """
        Like scan, but in reverse order.
        """
        return self.execute_command('rscan', start,end,limit)

    def multi_set(self, kvs):
        """
        Set multiple key-value pairs(kvs) in one method call.
        """
        params = []
        for kv in kvs.items():
            params.extend(kv)
        return self.execute_command('multi_set', *params)

    def multi_get(self, keys):
        """
        Get the values related to the specified multiple keys
        """
        return self.execute_command('multi_get', *keys)

    def multi_del(self, keys):
        """
        Delete specified multiple keys.
        """
        return self.execute_command('multi_del', *keys)

    # Hashmap
    def hset(self, name, key, value):
        """
        Set the string value in argument as value of the key of a hashmap.
        """
        return self.execute_command('hset', name, key, value)

    def hget(self, name, key):
        """
        Get the value related to the specified key of a hashmap
        """
        return self.execute_command('hget', name, key)

    def hdel(self, name, *keys):
        """
        Delete specified key of a hashmap. To delete the whole hashmap, use hclear.
        """
        return self.execute_command('hdel', name, *keys)

    def hincr(self, name, key, amount=1):
        """
        Increment the number stored at key in a hashmap by num. The num argument could be a negative integer.
        The old number is first converted to an integer before increment,
        assuming it was stored as literal integer.
        """

        return self.execute_command('hincr', name, key, amount)

    def hexists(self, name, key):
        """
        Verify if the specified key exists in a hashmap.
        """
        return self.execute_command('hexists', name, key)

    def hsize(self, name):
        "Return the number of key-value pairs in the hashmap."
        return self.execute_command('hsize', name)

    def hlist(self, start='',end='',limit=-1):
        """
        List hashmap names in range (name_start, name_end].
        ("", ""] means no range limit.
        Refer to scan command for more information about how it work.
        """
        return self.execute_command('hlist', start,end,limit)

    def hrlist(self, start='',end='',limit=-1):
        "Like hlist, but in reverse order."
        return self.execute_command('hrlist', start,end,limit)

    def hkeys(self,name, start='',end='',limit=-1):
        """
        List keys of a hashmap in range (start, end].
        ("", ""] means no range limit.
        """
        return self.execute_command('hkeys',name, start,end,limit)

    def hgetall(self, name):
        "Returns the whole hash, as an array of strings indexed by strings."
        return self.execute_command('hgetall', name)

    def hscan(self,name, start='', end='', limit=-1):
        """
        List key-value pairs of a hashmap with keys in range (start, end].
        ("", ""] means no range limit.
        Refer to scan command for more information about how it work.
        """
        return self.execute_command('hscan',name, start,end,limit)

    def hrscan(self,name, start='', end='', limit=-1):
        """
        Like hscan, but in reverse order.
        """
        return self.execute_command('hrscan',name, start,end,limit)

    def hclear(self,name):
        """
        Delete all keys in a hashmap.
        """
        return self.execute_command('hclear',name)

    def multi_hset(self,name, hkvs):
        """
        Set multiple key-value pairs(kvs) of a hashmap in one method call.
        """
        params = []
        for kv in hkvs.items():
            params.extend(kv)
        return self.execute_command('multi_hset',name, *params)

    def multi_hget(self,name, keys):
        """
        Get the values related to the specified multiple keys of a hashmap.
        """
        return self.execute_command('multi_hget',name, *keys)

    def multi_hdel(self,name, keys):
        """
        Delete specified multiple keys in a hashmap.
        """
        return self.execute_command('multi_hdel',name, *keys)

    # Sorted Set
    def zset(self, name, key, score):
        """
        Set the score of the key of a zset.
        """
        return self.execute_command('zset', name, key,score)

    def zget(self, name, key):
        """
        Get the score related to the specified key of a zset
        """
        return self.execute_command('zget', name, key)

    def zdel(self, name, key):
        """
        Delete specified key of a zset.
        """
        return self.execute_command('zdel', name, key)

    def zincr(self, name, key,num):
        """
        Increment the number stored at key in a zset by num.
        """
        return self.execute_command('zincr', name, key,num)

    def zexists(self, name, key):
        """
        Verify if the specified key exists in a zset.
        """
        return self.execute_command('zexists', name, key)

    def zsize(self, name):
        """
        Return the number of pairs of a zset.
        """
        return self.execute_command('zsize', name)

    def zlist(self, start='',end='',limit=-1):
        """
        List zset names in range (start, end].
        Refer to scan command for more information about how it work.
        """
        return self.execute_command('zlist', start,end,limit)

    def zrlist(self, start='',end='',limit=-1):
        """
        List zset names in range (start, end], in reverse order.
        """
        return self.execute_command('zrlist', start,end,limit)

    def zkeys(self,name,key_start, score_start='',score_end='',limit=-1):
        "List keys in a zset."
        return self.execute_command('zkeys',name,key_start, score_start,score_end,limit)

    def zscan(self,name,key_start, score_start='',score_end='',limit=-1):
        """
        List key-score pairs where key-score in range (key_start+score_start, score_end].
        Refer to scan command for more information about how it work.
        """
        return self.execute_command('zscan',name,key_start, score_start,score_end,limit)

    def zrscan(self,name,key_start, score_start='',score_end='',limit=-1):
        "List key-score pairs of a zset, in reverse order. See method zkeys()."
        return self.execute_command('zrscan',name,key_start, score_start,score_end,limit)

    def zrank(self,name,key):
        "Returns the rank(index) of a given key in the specified sorted set."
        return self.execute_command('zrank',name,key)

    def zrrank(self,name,key):
        "Returns the rank(index) of a given key in the specified sorted set, in reverse order."
        return self.execute_command('zrrank',name,key)

    def zrange(self,name,offset,limit):
        "Returns a range of key-score pairs by index range [offset, offset + limit)."
        return self.execute_command('zrange',name,offset,limit)

    def zrrange(self,name,offset,limit):
        """
        Returns a range of key-score pairs by index range [offset, offset + limit), in reverse order.
        """
        return self.execute_command('zrrange',name,offset,limit)

    def zclear(self,name):
        "Delete all keys in a zset."
        return self.execute_command('zclear',name)

    def zcount(self,name,start,end):
        """
        Returns the number of elements of the sorted set stored at the specified key
        which have scores in the range [start,end].
        """
        return self.execute_command('zcount',name,start,end)

    def zsum(self,name,start,end):
        """
        Returns the sum of elements of the sorted set stored at the specified key
        which have scores in the range [start,end].
        """
        return self.execute_command('zsum',name,start,end)

    def zavg(self,name,start,end):
        """
        Returns the average of elements of the sorted set stored at the specified key
        which have scores in the range [start,end].
        """
        return self.execute_command('zavg',name,start,end)

    def zremrangebyrank(self,name,start,end):
        """
        Delete the elements of the zset which have rank in the range [start,end].
        """
        return self.execute_command('zremrangebyrank',name,start,end)

    def zremrangebyscore(self,name,start,end):
        """
        Delete the elements of the zset which have score in the range [start,end].
        """
        return self.execute_command('zremrangebyscore',name,start,end)

    def zpop_front(self,name,limit):
        """
        Delete and return limit element(s) from front of the zset.
        """
        return self.execute_command('zpop_front',name,limit)

    def zpop_back(self,name,limit):
        """
        Delete and return limit element(s) from back of the zset.
        """
        return self.execute_command('zpop_back',name,limit)

    def multi_zset(self,name, zkvs):
        """
        Set multiple key-score pairs(kvs) of a zset in one method call.
        """
        params = []
        for kv in zkvs.items():
            params.extend(kv)
        return self.execute_command('multi_zset',name, *params)

    def multi_zget(self,name, keys):
        """
        Get the values related to the specified multiple keys of a zset.
        """
        return self.execute_command('multi_zget',name, *keys)

    def multi_zdel(self,name, keys):
        """
        Delete specified multiple keys of a zset.
        """
        return self.execute_command('multi_zdel',name, *keys)

    # List

    def qpush_front(self,name,items):
        """
        Add one or more than one element to the head of the queue.
        """
        return self.execute_command('qpush_front',name,*items)

    def qpush_back(self,name,items):
        """
        Add an or more than one element to the end of the queue.
        """
        return self.execute_command('qpush_back',name,*items)

    def qpop_front(self,name,size):
        """
        Pop out one or more elements from the head of a queue.
        """
        return self.execute_command('qpop_front',name,size)

    def qpop_back(self,name,size):
        """
        Pop out one or more elements from the tail of a queue.
        """
        return self.execute_command('qpop_back',name,size)

    # Alias of qpush_back
    qpush = qpush_back
    # Alias of qpop_front
    qpop = qpop_front

    def qfront(self,name):
        """
        Returns the first element of a queue.
        """
        return self.execute_command('qfront',name)

    def qback(self,name):
        """
        Returns the last element of a queue.
        """
        return self.execute_command('qback',name)

    def qsize(self,name):
        """
        Returns the number of items in the queue.
        """
        return self.execute_command('qsize',name)

    def qclear(self,name):
        """
        Clear the queue.
        """
        return self.execute_command('qclear',name)

    def qget(self,name,index):
        """
        Returns the element a the specified index(position).
        0 the first element, 1 the second ... -1 the last element.
        """
        return self.execute_command('qget',name,index)

    def qset(self,name,index,value):
        """
        Sets the list element at index to value. An error is returned for out of range indexes.
        """
        return self.execute_command('qset',name,index,value)

    def qrange(self,name,offset,limit):
        """
        Returns a portion of elements from the queue at the specified range [offset, offset + limit].
        """
        return self.execute_command('qrange',name,offset,limit)

    def qslice(self,name,begin,end):
        """
        Returns a portion of elements from the queue at the specified range [begin, end].
        begin and end could be negative.
        """
        return self.execute_command('qslice',name,begin,end)

    def qtrim_front(self,name,size):
        """
        Remove multi elements from the head of a queue.
        """
        return self.execute_command('qtrim_front',name,size)

    def qtrim_back(self,name,size):
        """
        Remove multi elements from the tail of a queue.
        """
        return self.execute_command('qtrim_back',name,size)

    def qlist(self,start,end,limit):
        """
        List list/queue names in range (start, end].
        ("", ""] means no range limit.
        Refer to scan command for more information about how it work.
        """
        return self.execute_command('qlist',start,end,limit)

    def qrlist(self,start,end,limit):
        """
        Like qlist, but in reverse order.
        """
        return self.execute_command('qrlist',start,end,limit)
