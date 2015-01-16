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
logger = logging.getLogger(__name__)
cwd = Path(__file__).absolute().ancestor(1)


class Searcher(object):
    def __init__(self):
        self.cwd = Path(__file__).absolute().ancestor(1)
        self.dict_id = defaultdict(dict)
        self.dict_sug_idx = defaultdict(set)
        self.dict_se_idx = defaultdict(set)
        self.max_id = 0
        self.load()
        
    def load(self, item_id_pic = './data/item_id.pic', sug_idx_pic='./data/item_sug_idx.pic', se_idx_pic='./data/item_se_idx.pic'):
        try:
            self.dict_id = cPickle.load(open(Path(self.cwd, item_id_pic)))
            self.dict_sug_idx = cPickle.load(open(Path(self.cwd, sug_idx_pic)))
            self.dict_se_idx = cPickle.load(open(Path(self.cwd, se_idx_pic)))
        except:
            self.dict_id = cPickle.load(open(Path(self.cwd, item_id_pic)))
            self.build_sug_idx(sug_idx_pic)
            self.build_se_idx(se_idx_pic)
        if self.dict_id:
            self.max_id = max(self.dict_id)
        logger.error('save dict_id %s dict_sug_idx %s dict_se_idx %s max_id %s' % (len(self.dict_id), len(self.dict_sug_idx), len(self.dict_se_idx), self.max_id))
    
        
    def build_sug_idx(self, sug_idx_pic=''):
        for id,item in self.dict_id.iteritems():
            name = item['name'].lower() 
            if name in [u'金箍棒ol', u'女神的远征']:
                logger.error(item)
            for i in range(len(name)):
                self.dict_sug_idx[name[:i]].add(name)
            list_pinyin = YhPinyin.yhpinyin.line2py_list(name)
            for i in range(1, min(6, len(list_pinyin)+1)):
                k_pinyin = ''.join([list_pinyin[j][0] for j in range(i)])
                self.dict_sug_idx[k_pinyin].add(name)
            str_pinyin = ''.join(list_pinyin)
            for i in range(1,6):
                self.dict_sug_idx[str_pinyin[:i]].add(name)
        cPickle.dump(self.dict_sug_idx, open(Path(self.cwd, sug_idx_pic), 'w+'))
    
    def build_se_idx(self, se_idx_pic=''):
        for id, item in self.dict_id.iteritems():
            name = item['name']
            name = name.lower() 
            if name in [u'金箍棒ol', u'女神的远征']:
                logger.error(item)
            self.dict_se_idx[name].add(item['id'])
            list_part = YhChineseNorm.string2ListBigram(name)
            for part in list_part:
                self.dict_se_idx[part].add(item['id'])
        cPickle.dump(self.dict_se_idx, open(Path(self.cwd, se_idx_pic), 'w+'))
        
    def suggest(self, query='', start=0, num=20, cache=0):
        try:
            if isinstance(query, str):
                query = unicode(query, 'utf8', 'ignore')
            list_url, num_url = [], 0
            if query:
                set_url = self.dict_sug_idx[query.lower()]
                list_url = list(set_url)
                list_url.sort()
            else:
                raise
            logger.error('list_url query %s res %s' % (query, len(list_url)))
            dict_res = {'query':query, 'sug':list_url[start:start+num], 'status':0, 'totalnum':len(list_url)}
            return dict_res
        except:
            dict_res={'status':2, 'errlog':traceback.format_exc()}
            logger.error(traceback.format_exc())
            return dict_res
    
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
                logger.error('matched id %s name %s' % (l['id'], l['name']))
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
        
    def get_maxid(self):
        return {'maxid':self.max_id}
        
searcher = Searcher()
class Search_Handler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    @tornado.gen.engine
    def get(self):
        try:
            dict_qs = YhTool.yh_urlparse_params(self.request.uri, ['query', 's', 'n', 'cache'], ['', '0', '20', '1'] )
            query, start, num,cache = dict_qs['query'], int(dict_qs['s']), int(dict_qs['n']), int(dict_qs['cache'])
            logger.error('%s\t%s\t%s\t%s' % (query, start, num, cache))
            dict_res = searcher.search(query, start, num, cache)
            logger.error('search %s\t%s\t%s\t%s  %s' % (query, start, num, cache, len(dict_res['data'])))
            self.set_header('Content-Type', 'application/json; charset=UTF-8')
            self.write(json_encode(dict_res))
        except Exception:
            logger.error('svs_handler error time[%s][%s][%s]'% (self.request.request_time(), traceback.format_exc(), self.request.uri))
            self.write(json_encode({'status':1, 'errlog':traceback.format_exc(), 'url':self.request.uri}))
        finally:
            self.finish()
            
class Suggest_Handler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    @tornado.gen.engine
    def get(self):
        try:
            dict_qs = YhTool.yh_urlparse_params(self.request.uri, ['query', 's', 'n', 'cache'], ['', '0', '20', '1'] )
            query, start, num,cache = dict_qs['query'], int(dict_qs['s']), int(dict_qs['n']), int(dict_qs['cache'])
            logger.error('%s\t%s\t%s\t%s' % (query, start, num, cache))
            self.set_header('Content-Type', 'application/json; charset=UTF-8')
            self.write(json_encode(searcher.suggest(query, start, num, cache)))
        except Exception:
            logger.error('svs_handler error time[%s][%s][%s]'% (self.request.request_time(), traceback.format_exc(), self.request.uri))
            self.write(json_encode({'status':1, 'errlog':traceback.format_exc(), 'url':self.request.uri}))
        finally:
            self.finish()            
            
class Info_Handler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    @tornado.gen.engine
    def get(self):
        try:
            dict_qs = YhTool.yh_urlparse_params(self.request.uri, ['query', 's', 'n', 'cache'], ['', '0', '20', '1'] )
            query, start, num,cache = dict_qs['query'], int(dict_qs['s']), int(dict_qs['n']), int(dict_qs['cache'])
            logger.error('info_handler %s\t%s\t%s\t%s' % (query, start, num, cache))
            self.set_header('Content-Type', 'application/json; charset=UTF-8')
            self.write(json_encode(searcher.info(query, start, num, cache)))
        except Exception:
            logger.error('svs_handler error time[%s][%s][%s]'% (self.request.request_time(), traceback.format_exc(), self.request.uri))
            self.write(json_encode({'status':1, 'errlog':traceback.format_exc(), 'url':self.request.uri}))
        finally:
            self.finish()  
            
class SearchHtml_Handler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    @tornado.gen.engine
    def get(self):
        try:
            dict_qs = YhTool.yh_urlparse_params(self.request.uri, ['query', 's', 'n', 'cache'], ['', '0', '20', '1'] )
            query, start, num,cache = dict_qs['query'], int(dict_qs['s']), int(dict_qs['n']), int(dict_qs['cache'])
            logger.error('%s\t%s\t%s\t%s' % (query, start, num, cache))
            dict_tmp = searcher.search(query, start, num, cache)
            for item in dict_tmp['res']:
                logger.error(item.keys())
            self.render(template_name = 'xyse.html', list_res=dict_tmp['res'])
        except Exception:
            logger.error('svs_handler error time[%s][%s][%s]'% (self.request.request_time(), traceback.format_exc(), self.request.uri))
            self.write(simplejson.dumps({'status':1, 'errlog':traceback.format_exc(), 'url':self.request.uri}))
            self.finish()
        finally:
            pass

class MaxId_Handler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    @tornado.gen.engine
    def get(self):
        try:
            self.set_header('Content-Type', 'application/json; charset=UTF-8')
            dict_res = searcher.get_maxid()
            self.write(json_encode(dict_res))
        except Exception:
            logger.error('svs_handler error time[%s][%s][%s]'% (self.request.request_time(), traceback.format_exc(), self.request.uri))
            self.write(json_encode({'status':1, 'errlog':traceback.format_exc(), 'url':self.request.uri}))
        finally:
            self.finish()
    
if __name__=='__main__':
    Searcher().load()
    Searcher().search(sys.argv[1], int(sys.argv[2]), int(sys.argv[3]))
    #Searcher().suggest(sys.argv[1], int(sys.argv[2]), int(sys.argv[3]))