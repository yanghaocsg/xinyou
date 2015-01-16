#!/usr/bin/env python
#coding:utf8
import sys, re, logging, redis,traceback, time, os, simplejson, copy
from multiprocessing import Pool, Queue
from collections import defaultdict
from bson.binary import Binary
import string
from unipath import Path
import cPickle, lz4
from tornado.wsgi import WSGIContainer
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop
import tornado.gen, tornado.web

#self module
sys.path.append('../YhHadoop')
import YhLog, YhTool

logger = logging.getLogger(__file__)
class Info:
    def __init__(self, prefix_info='info:xinyou', prefix_id='id:xinyou', pic='./data/item_id.pic'):
        self.cwd = Path(__file__).absolute().ancestor(1)
        self.prefix_info = prefix_info
        self.prefix_id = prefix_id
        self.pic = pic
        self.dict_data = {}
        self.max_id = 0
        self.load_data()
        
    def load_data(self):
        try:
            self.dict_data = cPickle.load(open(Path(self.cwd, self.pic)))
            logger.error('load_data %s' % len(self.dict_data))
            logger.error(self.dict_data.keys())
            self.max_id = max(self.dict_data.keys())
            logger.error('max_id %s' % self.max_id)
        except:
            raise
            
    def getinfo(self, list_id=range(10)):
        list_data = []
        for id in list_id:
            id = int(id)
            if id in self.dict_data:
                list_data.append(self.dict_data[id])
        logger.error('getinfo %s %s' % (list_id[:3], list_data[:3]))
        return list_data
    
    def getmaxid(self):
        logger.error('getmaxid %s' % self.max_id)
        return self.max_id
    

    
        
info = Info()

if __name__=='__main__':
    info.getinfo()
    logger.error(info.getmaxid())
    #logger.error(info.getAuthorOne(info.getAuthorByBiz()))

    