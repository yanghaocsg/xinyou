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
from tornado.wsgi import WSGIContainer
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop
import tornado.gen, tornado.web
from tornado.escape import json_encode
#self module
sys.path.append('../YhHadoop')

import YhLog, YhMongo, YhTool

mongo = YhMongo.yhMongo.mongo_cli
logger = logging.getLogger(__file__)
#mongo.db['xinyou'].drop()
class DataItem:
    def __init__(self,db='xinyou',pic='./data/item_id.pic'):
        self.cwd = Path(__file__).absolute().ancestor(1)
        self.db=db
        mongo.db[self.db].ensure_index([('url', 1),('name', 1) ], unique=True,  background=True, dropDups =True)
        self.pic = pic
        
    def save(self, list_item=[{'url':'test', 'name':'', 'other':'abcd'}, {'url':'test1', 'name':'', 'other':''}]):
        if not isinstance(list_item, list):
            return {'error' : 'not list instance %s' % list_item}
        for d in list_item:
            try:
                e = {}
                for k,v in d.iteritems():
                    if k and v: 
                        e[k]=v
                mongo.db[self.db].update({'url':d['url']}, e, True)
            except:
                logger.error(traceback.format_exc())
        len_total = mongo.db[self.db].count()
        return {'count':len_total, 'update':len(list_item)}
        
    def dump(self):
        list_data = list(mongo.db[self.db].find({'name':{'$exists':True}, 'name':{'$exists':True}}))
        dict_data = {}
        set_name = set()
        for d in list_data:
            if d['name'] in set_name:
                continue
            d['_id']=''
            dict_data[d['id']]=d
            set_name.add(d['name'])
        cPickle.dump(dict_data, open(Path(self.cwd, self.pic),'w+'))
        logger.error('dump finished %s %s' % (self.pic, len(dict_data)))
    
            
#Save handler        
class Save_Handler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    @tornado.gen.engine
    def get(self):
        try:
            dict_qs = YhTool.yh_urlparse_params(self.request.uri, ['query', 's', 'n', 'cache'], ['', '0', '20', '1'] )
            query, start, num,cache = dict_qs['query'], int(dict_qs['s']), int(dict_qs['n']), int(dict_qs['cache'])
            logger.error('%s\t%s\t%s\t%s' % (query, start, num, cache))
            dict_res = DataItem().save(simplejson.loads(query))
            
            self.set_header('Content-Type', 'application/json; charset=UTF-8')
            self.write(json_encode(dict_res))
        except Exception:
            logger.error('svs_handler error time[%s][%s][%s]'% (self.request.request_time(), traceback.format_exc(), self.request.uri))
            self.write(json_encode({'status':1, 'errlog':traceback.format_exc(), 'url':self.request.uri}))
        finally:
            self.finish()
            
if __name__=='__main__':
    DataItem().dump()
