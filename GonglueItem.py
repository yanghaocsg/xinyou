#!/usr/bin/env python
#coding:utf8

import sys, re, logging, redis,traceback, time, os, simplejson, subprocess
from multiprocessing import Pool, Queue, Process
from collections import defaultdict
from bson.binary import Binary
import string
from unipath import Path
import cPickle, copy_reg
from bitstring import BitArray
import lz4,csv
#self module
sys.path.append('../YhHadoop')

import YhLog, YhMongo

mongo = YhMongo.yhMongo.mongo_cli
redis_zero = redis.Redis(port=7777, unix_socket_path='/tmp/redis.sock')
logger = logging.getLogger(__file__)
mongo.db['gonglue'].drop()
class GonglueItem:
    def __init__(self, data_csv = './data/gonglue_9game.csv'):
        self.cwd = Path(__file__).absolute().ancestor(1)
        self.db='gonglue'
        self.data_csv = data_csv
        mongo.db[self.db].ensure_index([('url', 1)], unique=True,  background=True, dropDups =True)
        
    def load_data(self):
        dict_data = {}
        num = mongo.db[self.db].count()
        logger.error('existed num %s' % num)
        list_dict = []
        for i, l in enumerate(csv.DictReader(open(Path(self.cwd, self.data_csv)))):
            l['id'] = num + i
            if 'url' not in l:
                l['url'] = '/gonglue_info?query=%s' % l['id']
            if 'tag' not in l:
                l['tag']='攻略'
            if 'summary' not in l:
                str_summary = unicode(l['content'][:200], 'utf8', 'ignore')
                str_summary = re.sub('<.*?>', '', str_summary)
                l['summary']=str_summary
            if 'keyword' not in l:
                str_keyword = unicode(l['title'][:200], 'utf8', 'ignore')
                l['keyword'] = re.sub('[_\s]*$', '', str_keyword)
            for k in l:
                if isinstance(l[k], str):
                    l[k] = l[k].strip()
            list_dict.append(l)
            
        logger.error('load from csv %s' % len(list_dict))
        self.save(list_dict)
        return list_dict
    
    def save(self, list_dict=[]):
        for d in list_dict:
            try:
                '''for k, v in d.iteritems():
                    if isinstance(v, str):
                        v = unicode(v, 'utf8', 'ignore')
                    if isinstance(k, str):
                        k = unicode(k, 'utf8', 'ignore')
                    logger.error('%s\t%s' % (k,v))
                '''
                mongo.db[self.db].insert(d)
            except:
                logger.error(traceback.format_exc())
        logger.error('save file %s num %s total %s' % (self.data_csv, len(list_dict), mongo.db[self.db].count()))
    
    def dump(self):
        list_data = list(mongo.db[self.db].find())
        for d in list_data:
            d['_id'] = ''
        cPickle.dump(list_data, open('./data/gonglue_item_all.pic', 'w+'))
        logger.error('dump %s' % len(list_data))

if __name__=='__main__':
    GonglueItem().load_data()
    GonglueItem('./data/mofang_gonglue.csv').load_data()
    GonglueItem().dump()