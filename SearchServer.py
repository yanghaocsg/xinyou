#!/usr/bin/env python
# -*- coding: UTF-8 -*-
from tornado.wsgi import WSGIContainer
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop
import tornado.gen, tornado.web
import eventlet, urllib
from eventlet.green import urllib2
from eventlet import Timeout

import sys, traceback, imp, os, subprocess, logging, datetime
from unipath import Path
import ConfigParser

sys.path.insert(0, "../YhHadoop")
#self module
import YhLog
#import PatternSearcher, PatternHandler, Searcher
import Searcher, DataItem
import GonglueSearcher
logger = logging.getLogger(__file__)

eventlet.monkey_patch()
logger.error('global init start [%s]\n====================='%datetime.datetime.now())
class root_handler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    @tornado.gen.engine
    def get(self):
        try:
            dict_res=dict()
            self.write('ok')
        except Exception as e:
            logger.error('root handler fail')
            self.write('<Html><Body>Server fail:'+str(e)+'</Body></Html>')
        finally:
            try:
                self.finish()
            except:
                pass

class reload_handler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    @tornado.gen.engine
    def get(self):
        try:
            query = self.get_argument('query')
            fp, pathname, description = imp.find_module(query)
            imp.load_module(query, fp, pathname, description)
            logger.error('reload %s ok' % query)
            self.write('reload %s ok' % query)
        except:
            msg_err = traceback.format_exc()
            logger.error('reload failed, %s' % msg_err) 
            self.write(msg_err)
        finally:
            try:
                self.finish()
            except:
                pass

class restart_handler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    @tornado.gen.engine
    def get(self):
        try:
            logger.error('restart ok')  
            self.write('%s restart begin' % os.getpid())
            IOLoop.instance().stop()
            for i in range(2):
                with Timeout(0.1):
                    logger.error('%s\t%s' % (os.getpid(), urllib2.urlopen('http://127.0.0.1:8889/restart').read()))
            self.finish()
            '''
            IOLoop.instance().stop()
            subprocess.call('supervisorctl restart se', shell=True)
            '''
        except:
            msg_err = traceback.format_exc()
            logger.error('restart failed, %s' % msg_err) 
            self.write(msg_err)
        finally:
            try:
                self.finish()
            except:
                pass

class wait_handler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    @tornado.gen.engine
    def get(self):
        try:
            for i in range(10000):
                logger.error(i)
            self.write('ok')
        except:
            logger.error(traceback.format_exc())
        finally:
            self.finish()
            logger.error('request_time %s [%s]' %(self.request.uri, self.request.request_time()))
            
class test_handler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
    @tornado.gen.engine
    def get(self):
        try:
            with Timeout(0.8, True):
                res = urllib2.urlopen('http://localhost:8889/wait').read()
            self.write('ok [%s]' % res)
        except:
            logger.error(traceback.format_exc())
        finally:
            self.finish()
            logger.error('request_time %s [%s]' %(self.request.uri, self.request.request_time()))
            
def multi_app():
    settings = {
    "static_path": os.path.join(os.path.dirname(__file__), "../static"),
    "template_path": Path(Path(__file__).ancestor(1), '../static'),
    #"cookie_secret": "61oETzKXQAGaYdkL5gEmGeJJFuYh7EQnp2XdTP1o/Vo=",
    #"login_url": "/login",
    #"xsrf_cookies": True,
    }   
    cwd = Path(__file__).absolute().ancestor(1)
    config = ConfigParser.ConfigParser()
    #config.read(Path(cwd, './conf/backend.conf'))
    port = 8889
    app = tornado.web.Application(handlers=[
        (r'/', root_handler),
        (r'/favicon.ico', root_handler),
        (r'/reload', reload_handler),
        (r'/restart', restart_handler),
        #(r'/se', PatternSearcher.Search_Handler),
        #(r'/sug', PatternSearcher.Suggest_Handler),
        #(r'/sehtml', PatternSearcher.SearchHtml_Handler),
        #(r'/info', PatternHandler.Content_Handler),
        #(r'/infohtml', PatternHandler.ContentHtml_Handler),
        (r'/newse', Searcher.Search_Handler),
        (r'/newsug', Searcher.Suggest_Handler),
        (r'/newinfo', Searcher.Info_Handler),
        (r'/newmaxid', Searcher.MaxId_Handler),
        #(r'/gonglue_se', GonglueSearcher.GonglueSearcher_Handler),
        #(r'/gonglue_info', GonglueSearcher.GonglueSearcher_Info_Handler),
        #(r'/gonglue_browse', GonglueSearcher.GonglueSearcher_Browse_Handler),
        (r'/data_save', DataItem.Save_Handler),
        (r'/test', test_handler),
        (r'/wait', wait_handler),
        ], **settings)
    http_server = HTTPServer(app)
    http_server.bind(port)
    http_server.start()
    logger.error('listen port %s' % port)
    IOLoop.instance().start()
    
        
        
if __name__ == '__main__':
    pid = os.fork()
    if(pid >0):
        os._exit(0)
    multi_app()
