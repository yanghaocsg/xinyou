#coding:utf8
import HTMLParser
from htmlentitydefs import entitydefs 
import urllib2
from unipath import Path

class TitleParser(HTMLParser.HTMLParser): 
    def __init__(self): 
        self.taglevels=[] 
        self.handledtags=['div'] #提出标签 
        self.processing=None 
        self.find=0
        HTMLParser.HTMLParser.__init__(self) 
        
    def handle_starttag(self, tag, attrs):
        if tag in ['a']:
            if ('href', 'javasrcipt:;') in attrs and ('target', '_self') in attrs:
                print "Start tag:", tag
            self.find=1
    def handle_endtag(self, tag):
        if self.find == 1:
            if tag=='div'
                print "End tag  :", tag
        
    def handle_data(self, data):
        print "Data     :", data
        
    def handle_comment(self, data):
        print "Comment  :", data
        
    def handle_entityref(self,name): 
        if entitydefs.has_key(name): 
            self.handle_data(entitydefs[name]) 
        else: 
            self.handle_data('&'+name+';') 
        
    def handle_charref(self, name):
        if name.startswith('x'):
            c = unichr(int(name[1:], 16))
        else:
            c = unichr(int(name))
        print "Num ent  :", c
    
    def handle_decl(self, data):
        print "Decl     :", data

    
    
def getimage(addr):#提取图片并存在当前目录下 
    data = urllib2.urlopen(addr).read() 
    filename=addr.split('/')[-1] 
    f=open(Path(Path(__file__).ancestor(1), filename),'wb') 
    f.write(data) 
    f.close() 
    print filename+'已经生成！' 
    return data
    
    
buf = '<html> <head> <title> XHTML 与 HTML 4.01 标准没有太多的不同</title> </head> \
        <body> i love you <a href="http://pypi.python.org/pypi" title="link1">i love&#247; you&times;</a> \
        <div id="m"><img src="http://www.baidu.com/img/baidu_sylogo1.gif" width="270" height="129" ></div> </body> </html>'

data = getimage('http://game.mofang.com/info/58671.html')
data = open(Path(Path(__file__).ancestor(1),'58671.html')).read()
tp=TitleParser() 
tp.feed(data) 