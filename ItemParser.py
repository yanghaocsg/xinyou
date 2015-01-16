#!/usr/bin/env python
#coding:utf8

import eventlet
from eventlet.green import urllib2
import sys, re, logging, redis,traceback, time
import multiprocessing, os, cPickle
from unipath import Path
from HTMLParser import HTMLParser
#self module
sys.path.append('/data/CloudSE/YhHadoop')
import YhLog, lz4


logger = logging.getLogger(__file__)


class ItemParser:
    def __init__(self):
        self.cwd = Path(__file__).ancestor(1)
    
    def parse(self, content=''):
        list_url = re.findall(url_pattern, content, flags=re.M|re.I)
        #logger.error('\n'.join(list_url))
        return list_url
    
    def process(self, ifn_pic='./data/mofang_info_item.pic', ofn_pic='./data/mofang_item_list.pic'):
        dict_kv = cPickle.load(open(ifn_pic))
        list_item = []
        for k, v in dict_kv.iteritems():
            logger.error('%s\t%s\t%s' % (k, type(v), v[:100]))
            parser = HTMLParser()
            parser.feed(v)
            logger.error(parser.get_starttag_text())
            break
        #logger.error('%s\t%s' % (len(set_url), '\n'.join(list(set_url)[:3])))
        cPickle.dump(list_item, open(Path(self.cwd, ofn_pic), 'w+'))
        
itemParser = ItemParser()    
if __name__=='__main__':
    itemParser.process()