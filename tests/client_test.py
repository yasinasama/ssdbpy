# -*- coding: utf-8 -*-

from ssdb import SSDB


if __name__=='__main__':
    r = SSDB(host='10.68.120.175',port=7777,password='11111111111111111111111111111111')
    # r = SSDB.from_url(url='ssdb://:11111111111111111111111111111111@10.68.120.175:7777')
    # **Server**
    # b = r.auth('11111111111111111111111111111111')
    # b=r.info('cmd')
    # b= r.dbsize()
    # b = r.flushdb()
    # **Key Value**
    # b=r.set('aa',11)                          # 1
    # b=r.setx('aa',11,1000)                    # 1
    # b = r.setnx('bds', 11)                    # 1/0
    # b = r.expire('aaqe',1000)                 # 1/0
    # b = r.ttl('bdasd')                        # int value
    b = r.get('bdas')                         # value/none
    # b = r.getset('bdadssf','22')              # value/none
    # b = r.delete('aa')                        # 1
    # b = r.incr('aaaasdfsd',2)                 # int value
    # b = r.exists('aaa')                       # true/false
    # b = r.getbit('14aaasdf',11)               # 1/0
    # b = r.setbit('asfga',11112341, 1)         # 1/0
    # b = r.bitcount('aavba', 0,1)              # 1/0
    # b = r.countbit('aa',0,-1)                 # 1/0
    # b = r.substr('bdadssf',0,-1)              # value
    # b = r.strlen('asfga')                     # int value
    # b = r.keys('','aavba',10)                 # list
    # b = r.rkeys('','',10)                     # list
    # b = r.scan('','',-1)                      # dict
    # b = r.rscan('', '', -1)                   # dict
    # kvs = {
    #     'w1':'w1',
    #     'w2': 'w2',
    #     'w3': 'w3',
    #     'w4': 'w4'
    # }
    # b = r.multi_set(kvs)                      # int value
    # keys = ['w1','w2','t3','t4']
    # b = r.multi_get(keys)                     # dict
    # keys = ['w1','w2']
    # b = r.multi_del(keys)                     # int value

    # **Hashmap**
    # b = r.hset('user:1','name','cj2')         # 0
    # b = r.hget('user:1', 'nameas')            # value/none
    # b = r.hdel('user:1','name','nam')         # int value
    # b = r.hincr('user:1', 'age2',10)          # int value
    # b = r.hexists('user:001','age')           # true/false
    # b = r.hsize('user:1')                     # int value
    # b = r.hlist('','',-1)                     # list
    # b = r.hrlist('', '', -1)                  # list
    # b = r.hkeys('user:1','', '', -1)         c
    # b = r.hgetall('user22:001')               # dict
    # b = r.hrscan('user:001','', '', -1)       # dict
    # b = r.hclear('user:001')                  # int value
    # hkvs = {
    #         'w1':'w1',
    #         'w2': 'w2',
    #         'w3': 'w3',
    #         'w4': 'w4'
    #     }
    # b = r.multi_hset('user:002',hkvs)         # int value
    # hkeys = ['w1', 'w2', 't3', 't4']
    # b = r.multi_hget('user:0022',hkeys)       # dict
    # hkeys = ['w1','w2']
    # b = r.multi_hdel('user:002',hkeys)        # int value

    # **Sorted Set**
    # b = r.zset('rank','score','50')           # 1/0
    # b = r.zget('rank', 'score')               # value/none
    # b = r.zdel('rank', 'score')               # int value
    # b = r.zincr('rank', 'score',50)           # int value
    # b = r.zexists('rank', 'scoe')             # true/false
    # b = r.zsize('rank')                       # int value
    # b = r.zlist('','',-1)                     # list
    # b = r.zrlist('', '', -1)                  # list
    # b = r.zkeys('score2','',0,100,-1)         # list
    # b = r.zscan('score', '', 0, 100, -1)      # dict
    # b = r.zrscan('score', '', 100, 0, -1)     # dict
    # b = r.zrank('rank', 'score')              # value/none
    # b = r.zrrank('score', 'cj')               # value/none
    # b = r.zrange('scoread', 13,23)                # dict
    # b = r.zrrange('score', 0, 2)                  # dict
    # b = r.zclear('score1')                    # int value
    # b = r.zcount('score1',0,1000)               #int value
    # b = r.zsum('score', 0, 1000)              #int value
    # b = r.zavg('score', 0, 1000)              # float value
    # b= r.zremrangebyrank('scort', 0, 1000)    # int value
    # b = r.zremrangebyscore('score', 0, 1000)  # int value
    # b = r.zpop_front('score', 10)               #dict
    # b = r.zpop_back('score', 10)              #dict
    # zkvs = {
    #         'w1':10,
    #         'w2': 20,
    #         'w3': 30,
    #         'w4': 40
    #     }
    # b = r.multi_zset('score',zkvs)            # int value
    # zkeys = ['w1', 'w2', 't3', 't4']
    # b = r.multi_zget('scorwe',zkeys)            #dict
    # zkeys = ['w1','w2']
    # b = r.multi_zdel('score1',zkeys)            # int value

    # **List**
    # items = ['a','b','c']
    # b = r.qpush_front('list1',items)          # int value
    # items = ['a', 'b', 'c']
    # b = r.qpush_back('list1', items)          # int value
    # b = r.qpop_front('list111', 2)            # list
    # b = r.qpop_back('list1', 2)               # list
    # items = ['1', '2', '3']
    # b = r.qpush('list1', items)               # int value
    # b = r.qpop('list1', 2)                    # list
    # b = r.qfront('list1')                     # value/none
    # b = r.qback('list11')                      # value/none
    # b = r.qsize('list1')                     # int value
    # b = r.qclear('list1')                     # int value
    # b = r.qget('list1',2)                   # value/none
    # b = r.qset('list1', 2,9)
    # b = r.qrange('list1', 1, 6)                # list
    # b = r.qslice('list1', 1, 6)                # list
    # b = r.qtrim_front('list1', 2)         # int value
    # b = r.qtrim_back('list1', 2)          # int value
    # b = r.qlist('','',-1)                 # list
    # b = r.qrlist('', '', -1)              # list
    print(b)
