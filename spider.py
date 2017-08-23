# -*- coding: utf-8 -*-

import json
import re
import multiprocessing as multi
from urllib import urlencode
import pymongo
import requests
import time
import sys
from bs4 import BeautifulSoup
from requests import ConnectionError

reload(sys)
sys.setdefaultencoding('utf-8')

client = pymongo.MongoClient( 'localhost', connect=False)
db = client['souhu']

headers = {
        'User - Agent': 'Mozilla / 5.0(Windows NT 6.1;WOW64) AppleWebKit / 537.36(KHTML, likeGecko) Chrome / 59.0.3071.86 Safari / 537.36'
    }
def sceneIds():
    for sceneId in range(8,31):
        yield sceneId

'''判断sceneId，根据不同sceneId保存到不同的表'''
def MONGO_TABLES(sceneId):
    if sceneId == 8:
        MONGO_TABLE = '新闻'
        return  MONGO_TABLE
    elif sceneId == 10:
        MONGO_TABLE = '军事'
        return  MONGO_TABLE
    elif sceneId == 12:
        MONGO_TABLE = '文化'
        return  MONGO_TABLE
    elif sceneId == 13:
        MONGO_TABLE = '历史'
        return  MONGO_TABLE
    elif sceneId == 15:
        MONGO_TABLE = '财经'
        return  MONGO_TABLE
    elif sceneId == 17:
        MONGO_TABLE = '体育'
        return  MONGO_TABLE
    elif sceneId == 18:
        MONGO_TABLE = '起车'
        return  MONGO_TABLE
    elif sceneId == 19:
        MONGO_TABLE = '娱乐'
        return  MONGO_TABLE
    elif sceneId == 23:
        MONGO_TABLE = '时尚'
        return  MONGO_TABLE
    elif sceneId == 24:
        MONGO_TABLE = '健康'
        return  MONGO_TABLE
    elif sceneId == 25:
        MONGO_TABLE = '教育'
        return  MONGO_TABLE
    elif sceneId == 27:
        MONGO_TABLE = '星座'
        return  MONGO_TABLE
    elif sceneId == 28:
        MONGO_TABLE = '没事'
        return  MONGO_TABLE
    elif sceneId == 29:
        MONGO_TABLE = '旅游'
        return  MONGO_TABLE
    elif sceneId == 30:
        MONGO_TABLE = '科技'
        return  MONGO_TABLE

def souhu_index(sceneId,page):
    try:
        data = {
                'scene': 'CHANNEL',
                 'sceneId': sceneId,
                 'page': page,
                'size': '20',
                 '_': '1500533123044'
                }
        params = urlencode(data)  ##利用urllib中的urlencode来构建data
        urls = 'http://v2.sohu.com/public-api/feed'
        url = urls + '?' + params
        response = requests.get(url, headers)
        time.sleep(3)
        response.encoding = response.apparent_encoding  # 默认编码改为UTF-8
        if response.status_code == 200:  # 检测返回码是否200
            return response.text
        return None

    except ConnectionError:
        pass

def souhu_page_index(text):
    try:
        data = json.loads(text)
        for i in data:
            ids =  i['id']
            authorIds = i['authorId']
            page_url ='http://www.sohu.com/a/'+ str(ids)+'_'+str(authorIds)
            print page_url
            if db['page_url'].find_one({'url': page_url}):  # url去重
                print '%s这url爬过'%page_url
            else:
                yield page_url


    except :
        pass

def souhu_content( MONGO_TABLE,page_url):
    try:
        response = requests.get(page_url, headers)
        time.sleep(3)
        response.encoding = response.apparent_encoding  # 默认编码改为UTF-8
        if response.status_code == 200:  # 检测返回码是否200
            text = re.sub('<p data-role="original-title" style="display:none">.*?</p>', '', response.text)
            text = re.sub(
                '<a href=".*? " target="_blank" title=".*?" id="backsohucom" style="white-space: nowrap;">'
                '<span class="backword"><i class="backsohu"></i>.*?</span></a></p>', '', text)
            text = re.sub('<p data-role="editor-name">.*?<span></span></p>', '', text)
            text = re.sub('\n','',text)
            soup = BeautifulSoup(text, 'lxml')
            title = soup.find_all('h1')
            news_time = soup.select('#news-time')
            author = soup.select('#user-info > h4 > a')
            text = soup.select('#article-container > div.left.main > div.text > article')
            for title, news_time, author, text in zip(title, news_time, author, text):
                data = {
                    'title': title.get_text(),
                    'news_time': news_time.get_text(),
                    'author': author.get_text(),
                    'text': text.get_text().strip(),
                    'page_url': page_url

                }
                data_url = {'url': page_url}
                db[MONGO_TABLE].insert(data)
                db['page_url'].insert(data_url)

        return None
    except ConnectionError:
        print 'Error occurred'
        return None




def main(page):
    for sceneId in sceneIds():
        MONGO_TABLE = MONGO_TABLES(sceneId)
        text = souhu_index(sceneId, page)
        page_url = souhu_page_index(text)
        for page_url in page_url:
            souhu_content(MONGO_TABLE, page_url)


if __name__ == '__main__':
    pool_num = 4
    pool = multi.Pool(pool_num)
    group = ([page for page in range(1,51)])
    pool.map(main,group)
    pool.close()
    pool.join()
