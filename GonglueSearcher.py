#!/usr/bin/env python
#coding:utf8

import sys, re, logging, redis,traceback, time, os, simplejson
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
from tornado.escape import json_encode


#self module
sys.path.append('../YhHadoop')
import YhLog, YhMongo, YhTool, YhChineseNorm, YhPinyin
logger = logging.getLogger(__file__)
cwd = Path(__file__).absolute().ancestor(1)


class GonglueSearcher(object):
    def __init__(self):
        self.cwd = Path(__file__).absolute().ancestor(1)
        self.dict_id = defaultdict(dict)
        self.dict_sug_idx = defaultdict(set)
        self.dict_se_idx = defaultdict(set)
        self.load()
        
    def load(self, item_pic = './data/gonglue_item_all.pic', item_id_pic = './data/gonglue_item_id.pic', se_idx_pic='./data/gonglue_item_se_idx.pic'):
        try:
            self.dict_id = cPickle.load(open(Path(self.cwd, item_id_pic)))
            self.dict_se_idx = cPickle.load(open(Path(self.cwd, se_idx_pic)))
        except:
            try:
                logger.error('load failed %s' % traceback.format_exc())
                list_item= cPickle.load(open(Path(self.cwd, item_pic)))
                logger.error('load list_item_pic %s %s' % (type(list_item), len(list_item)))
                logger.error('check _id %s' % list_item[0]['_id'])
                self.build_id(list_item, item_id_pic)
                self.build_se_idx(list_item, se_idx_pic)
            except:
                logger.error(traceback.format_exc())
        logger.error('save dict_id %s dict_se_idx %s' % (len(self.dict_id), len(self.dict_se_idx)))
    
    def build_id(self, list_item=[], item_id_pic=''):
        for item in list_item:
            self.dict_id[item['id']] = item
        cPickle.dump(self.dict_id, open(Path(self.cwd, item_id_pic), 'w+'))
        
    def build_se_idx(self, list_item=[], se_idx_pic=''):
        for item in list_item:
            name = item['title']
            name = name.lower() 
            if name in [u'金箍棒ol', u'女神的远征']:
                logger.error(item)
            self.dict_se_idx[name].add(item['id'])
            list_part = YhChineseNorm.string2ListBigram(name)
            for part in list_part:
                self.dict_se_idx[part].add(item['id'])
        cPickle.dump(self.dict_se_idx, open(Path(self.cwd, se_idx_pic), 'w+'))
    
    def get_info(self, list_id=[]):
        list_data = []
        for id in list_id:
            if id in self.dict_id:
                list_data.append(self.dict_id[id])
                logger.error('get_info %s' % type(self.dict_id[id]))
        return list_data
    
    
    def search(self, query='', start=0, num=20, cache=0):
        try:
            if isinstance(query, str):
                query = unicode(query, 'utf8', 'ignore')
            list_url, num_url = [], 0
            list_part = YhChineseNorm.string2ListBigram(query)
            set_id = set()
            list_matched = [self.dict_se_idx[p] for p in list_part if p in self.dict_se_idx]
            if list_matched:
                set_id = list_matched[0]
                for p in list_matched[1:]:
                    test_set = p & set_id
                    if not test_set: break
                    set_id = test_set
            list_id = list(set_id)
            list_id.sort()
            if len(set_id)<10 and list_matched:
                for id in list_part[0]:
                    if id not in set_id:
                        list_id.append(id)
            list_info = self.get_info(list_id)
            for l in list_info:
                logger.error('matched id %s title %s' % (l['id'], l['title']))
            return {'status':0, 'query':query, 'data':list_info[start:start+num],  'totalnum':len(list_info)}
        except:
            dict_res={'status':2, 'errlog':traceback.format_exc()}
            logger.error(traceback.format_exc())
            return dict_res
    
    def info(self, query='', start=0, num=20, cache=0):
        try:
            data = self.dict_id[int(query)]
            if data:
                return {'status':0, 'query':query, 'data':data}
            raise
        except:
            dict_res={'status':2, 'query':query, 'errlog':traceback.format_exc()}
            logger.error(traceback.format_exc())
            return dict_res
    
    def browse(self, query='', start=0, num=10, cache=0):
        try:
            list_data = []
            for i in range(start, start+num):
                data = self.dict_id[i]
                list_data.append(data)
            if list_data:
                return {'status':0, 'query':query, 'data':list_data, 'count':len(self.dict_id)}
            raise
        except:
            dict_res={'status':2, 'query':query, 'errlog':traceback.format_exc()}
            logger.error(traceback.format_exc())
            return dict_res
    
        
gonglueSearcher = GonglueSearcher()
class GonglueSearcher_Handler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    @tornado.gen.engine
    def get(self):
        try:
            dict_qs = YhTool.yh_urlparse_params(self.request.uri, ['query', 's', 'n', 'cache'], ['', '0', '20', '1'] )
            query, start, num,cache = dict_qs['query'], int(dict_qs['s']), int(dict_qs['n']), int(dict_qs['cache'])
            logger.error('%s\t%s\t%s\t%s' % (query, start, num, cache))
            dict_res = gonglueSearcher.search(query, start, num, cache)
            logger.error('search %s\t%s\t%s\t%s  %s' % (query, start, num, cache, len(dict_res['data'])))
            self.set_header('Content-Type', 'application/json; charset=UTF-8')
            self.write(json_encode(dict_res))
        except Exception:
            logger.error('svs_handler error time[%s][%s][%s]'% (self.request.request_time(), traceback.format_exc(), self.request.uri))
            self.write(json_encode({'status':1, 'errlog':traceback.format_exc(), 'url':self.request.uri}))
        finally:
            self.finish()
            
class GonglueSearcher_Info_Handler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    @tornado.gen.engine
    def get(self):
        try:
            dict_qs = YhTool.yh_urlparse_params(self.request.uri, ['query', 's', 'n', 'cache'], ['', '0', '20', '1'] )
            query, start, num,cache = dict_qs['query'], int(dict_qs['s']), int(dict_qs['n']), int(dict_qs['cache'])
            logger.error('info_handler %s\t%s\t%s\t%s' % (query, start, num, cache))
            self.set_header('Content-Type', 'application/json; charset=UTF-8')
            self.write(json_encode(gonglueSearcher.info(query, start, num, cache)))
        except Exception:
            logger.error('svs_handler error time[%s][%s][%s]'% (self.request.request_time(), traceback.format_exc(), self.request.uri))
            self.write(json_encode({'status':1, 'errlog':traceback.format_exc(), 'url':self.request.uri}))
        finally:
            self.finish()  

class GonglueSearcher_Browse_Handler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    @tornado.gen.engine
    def get(self):
        try:
            dict_qs = YhTool.yh_urlparse_params(self.request.uri, ['query', 's', 'n', 'cache'], ['', '0', '20', '1'] )
            query, start, num,cache = dict_qs['query'], int(dict_qs['s']), int(dict_qs['n']), int(dict_qs['cache'])
            logger.error('info_handler %s\t%s\t%s\t%s' % (query, start, num, cache))
            self.set_header('Content-Type', 'application/json; charset=UTF-8')
            self.write(json_encode(gonglueSearcher.browse(query, start, num, cache)))
        except Exception:
            logger.error('svs_handler error time[%s][%s][%s]'% (self.request.request_time(), traceback.format_exc(), self.request.uri))
            self.write(json_encode({'status':1, 'errlog':traceback.format_exc(), 'url':self.request.uri}))
        finally:
            self.finish()  

    
if __name__=='__main__':
    #Searcher().load()
    GonglueSearcher().search(sys.argv[1], int(sys.argv[2]), int(sys.argv[3]))
    #Searcher().suggest(sys.argv[1], int(sys.argv[2]), int(sys.argv[3]))