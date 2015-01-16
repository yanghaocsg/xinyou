#!/usr/bin/env python
#coding:utf8

import MySQLdb, pymongo,sys,traceback
from eventlet.green import urllib2
import urllib,simplejson
import logging
import calendar,datetime
#self module
logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.DEBUG)
logger = logging.getLogger(__file__)
def transfer():
    try:
        conn=MySQLdb.connect(host='localhost',user='root',passwd='123456',db='game',port=3306, charset="utf8")
        cur=conn.cursor()
        list_field = ['id', 'url', 'name', 'pubdate', 'publisher', 'tag', 'zutu', 'brief', 'img', 'platform']
        cur.execute('select %s from xinyou_url where not name is null and not pubdate is null' % ','.join(list_field))
        res = cur.fetchall()
        list_data = []
        for r in res:
            d ={}
            for k,v in zip(list_field, r):
                d[k] = v
            tdelta = 0
            try:
                tdelta = calendar.timegm(datetime.datetime.strptime(d['pubdate'].split()[0], '%Y-%m-%d').timetuple())
                d['timedelta']=tdelta
            except:
                logger.error(traceback.format_exc())
            list_data.append(d)
            if len(list_data)>=100:
                saveMongo(list_data)
                list_data = []
        saveMongo(list_data)
            
    except:
        logging.error(traceback.format_exc())

def saveMongo(list_data=[]):
    if list_data:
        url_data = 'http://219.239.89.185:8889/data_save?query=%s' % urllib.quote_plus(simplejson.dumps(list_data))
        res = urllib2.urlopen(url_data).read()
        logger.error('success %s' % res)
    else:
        logger.error('error %s' % list_data)
if __name__=='__main__':
    transfer()
    #saveMongo()