import sqlite3
import urllib.error
import ssl
from urllib.parse import urljoin
from urllib.parse import urlparse
from urllib.request import urlopen
from bs4 import BeautifulSoup

# Ignore SSL certificate errors
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

conn=sqlite3.connect('crawler.sqlite')

cur=conn.cursor()


cur.execute('Drop table if exists pages  ')
cur.execute('drop table if exists websites  ')
cur.execute('drop table if exists links')

# create table to store the url's and their html content and error 
# because we dont want to parse the error pages
cur.execute(''' 
    create table if not exists pages
    (id integer primary key autoincrement,url text unique,html text,error integer,old_rank real,new_rank real)
 ''')

# create table websites to store the particular links to ensure that the website we want to parse is stick with only one particular site
cur.execute(''' 
    create table if not exists websites(url text unique)
''')

cur.execute(''' 
    create table if not exists links(from_id integer,to_id integer)
''')

cur.execute(''' select id,url from pages where error is NULL and html is NULL ORDER BY RANDOM() LIMIT 1 ''')
row=cur.fetchone()

# for the first time insert start url in database
if row is None:
    starturl=input('Enter Url Or Press Enter - ')
    if len(starturl)<1:
        starturl='http://www.chitkara.edu.in/'
    if starturl.endswith('/'):
        starturl=starturl[:len(starturl)-1]
        # starturl=starturl[:-1]
    # print(starturl)
    website=starturl
    # it may be sub page
    if starturl.endswith('html') or starturl.endswith('htm'):
        pos=starturl.rfind('/')
        website=starturl[:pos]
    cur.execute(''' insert or ignore into websites(url) values(?)''',(website, ))
    cur.execute(''' insert or ignore into pages (url,html,new_rank) values(?,NULL,1.0)''',(starturl, ))
    conn.commit()
else:
    print('Restarting the crawler!! Delete crawler.sqlite file for fresh crawl')

cur.execute('select url from websites')
websites=[]
for row in cur:
    # here row will be tuple
    websites.append(str(row[0]))

# print('websites r',websites)

pageCount=0
while True:
    # sellect random pages which we want to retrieve
    if pageCount<1:
        pageCount=input('How many pages u want to crawl: ')
        if len(pageCount)<=0:
            break
        try:
            pageCount=int(pageCount)
        except:
            print('Pages data type is int')
            break
    if pageCount<=0:
        break
    pageCount=pageCount-1
    cur.execute('select url,id from pages where html is NULL and error is NULL order by random() limit 1')
    try:
        row=cur.fetchone()
        # print('row',row)
        from_id=str(row[1])
        notRetreived=str(row[0])
    except:
        print('No unretrieved Page is founded')
        break
    
    try:
        document=urlopen(notRetreived,context=ctx)
        if document.getcode()!=200:
            print('Error on page ',notRetreived)
            cur.execute('update pages set error=? where url=?',(document.getcode(), notRetreived))
            conn.commit()
        if document.info().get_content_type() !='text/html':
            print('Unable to retreive non html file')
            cur.execute('delete from pages where url=?',(notRetreived))
            conn.commit()
            continue

        html=document.read()
        soup=BeautifulSoup(html,'html.parser')

    except KeyboardInterrupt:
        print('Program Interupted by user:')
        break
    except:
        print('Unable to parse or retreive Document')
        cur.execute('Update pages set error=-1 where url=?',(notRetreived, ))
        conn.commit()
        continue

    cur.execute('insert or ignore into pages (url,html,error,new_rank) values(?,NULL,NULL,1.0)',(notRetreived, ))
    cur.execute('update pages set html=? where url=? ',(memoryview(html), notRetreived, ))
    print(notRetreived)
    conn.commit()
    tags=soup('a')
    # print(tags)
    for tag in tags:
        href=tag.get('href',None)
        if href is None:
            continue
        subUrl=href
        parsedUrl=urlparse(href)
        if len(parsedUrl.scheme)<1 :
            subUrl=urljoin(notRetreived,href)
        hashPos=subUrl.find('#')
        subUrl=subUrl[:hashPos]
        if subUrl.endswith('/'):
            subUrl=subUrl[:-1]
        if subUrl.endswith('gif') or subUrl.endswith('jpeg') or subUrl.endswith('jpg') or subUrl.endswith('doc') or subUrl.endswith('docx'):
            continue
        if len(subUrl)<1:
            continue
        found = False
        for website in websites:
            if subUrl.startswith(website):
                found=True
                break
        if found is False:
            continue
        # print('sub url is ****** ',subUrl)
        cur.execute('insert or ignore into pages (url,html,error,new_rank) values(?,NULL,NULL,1.0)',(subUrl, ))
        cur.execute('select id from pages where url=? LIMIT 1',(subUrl, ))
        try:
            row=cur.fetchone()
            to_id=row[0]
        except:
            print('Could not retreive id')
            continue
        cur.execute('insert or ignore into links (from_id,to_id) values(?,?)',(from_id,to_id))
        # pageCount=pageCount-1
        conn.commit()
cur.close()