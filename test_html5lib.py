#coding:utf8
from lxml import etree
import logging, sys
from eventlet.green import urllib2
sys.path.append('/data/CloudSE/YhHadoop')
import YhLog, lz4


logger = logging.getLogger(__file__)

def craw(url='http://www.mofang.com/kaice/2.html'):
    buf = urllib2.urlopen(url).read()
    open('./list_test.html', 'w+').write(buf)
    
def parse_mofang_kaice(buf):
    parser = etree.HTMLParser()
    doc = etree.fromstring(open('./list_test.html').read(), parser)
    for i in range(1,5):
        for j in doc.xpath('/html[1]/body[1]/div[3]/div[2]/div[1]/table[1]//tr/td[%s]' % i):
            list_text = list(j.itertext())
            logger.error('%s\t%s' % (i, ','.join(list_text).strip()))
    '''name, img, kaice_time, game_type, game_tag, game_and_ios=''*6
    for div in doc.xpath('body/div/div[@class="details-app pl-mb10"]'):
        logger.error(div.attrib)
        for t in div.xpath('//a[@class="details-app-img"]/img'):
            logger.error(t.get('src'))
        for t in div.xpath('//h3/a'):
            logger.error(t.text)
        for t in div.xpath('//p[@class="details-app-intro"]'):
            logger.error(t.text)
        for t in div.xpath('//ul[@class="details-app-about"]'):
            for tt in t.getchildren():
                logger.error('%s\t%s' % (tt.tag, tt.text))
            if tt.xpath('//span[@class="andd-logo"]'):
                logger.error('andd-logo')
        for t in div.xpath('//div[@class="details-app-tags"]'):
            logger.error(','.join([t.strip() for t in list(t.itertext()) if t.strip()]))
        for t in div.xpath('//div[@class="details-app-score"]/div[@class="score-box"]'):
            logger.error(','.join(list(t.itertext())))
    '''

if __name__=='__main__':
    #craw()
    parse_mofang_kaice('')