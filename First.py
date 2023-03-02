import os
import re  # 正则表达式提取文本
from jsonpath import jsonpath  # 解析json数据
import requests  # 发送请求
import pandas as pd  # 存取csv文件
import datetime  # 转换时间用

# !/usr/bin/python3
import requests
from bs4 import BeautifulSoup
import re
import pymysql
from sqlalchemy import create_engine


def create0():
    db = pymysql.connect(host='localhost', user='root', password='root', database='test')  # 连接数据库

    cursor = db.cursor()
    cursor.execute("DROP TABLE IF EXISTS USER")

    sql = """CREATE TABLE USER (
			AUID VARCHAR(255) PRIMARY KEY,
			NAME VARCHAR(2550),
			TIME VARCHAR(200))"""

    cursor.execute(sql)
    print("db ok")

    db.close()

def create1():
    db = pymysql.connect(host='localhost', user='root', password='root', database='test')  # 连接数据库

    cursor = db.cursor()
    cursor.execute("DROP TABLE IF EXISTS BLOG")

    sql = """CREATE TABLE BLOG (
			ID INT PRIMARY KEY AUTO_INCREMENT,
			TIME VARCHAR(200),
			TEXT VARCHAR(1000),
			REGION VARCHAR(255),
			CITY VARCHAR(255),
			PROVINCE VARCHAR(255),
			C VARCHAR(225))"""

    cursor.execute(sql)
    print("db ok")

    db.close()


def insert0(value):
    db = pymysql.connect(host='localhost', user='root', password='root', database='test')

    cursor = db.cursor()
    sql = "INSERT INTO USER(NAME,DESCRIBE1,TIME,TEXT,REGION,CITY,PROVINCE,C) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) "
    try:
        cursor.execute(sql, value)
        db.commit()
        print('插入数据成功')
    except:
        db.rollback()
        print("插入数据失败")
    db.close()

def insert1(value):
    db = pymysql.connect(host='localhost', user='root', password='root', database='test')

    cursor = db.cursor()
    sql = "INSERT INTO BLOG(TIME,TEXT,REGION,CITY,PROVINCE,C) VALUES (%s, %s, %s, %s, %s, %s) "
    try:
        cursor.execute(sql, value)
        db.commit()
        print('插入数据成功')
    except:
        db.rollback()
        print("插入数据失败")
    db.close()


# 请求头
headers = {
    "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Mobile Safari/537.36",
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "accept-encoding": "gzip, deflate, br",
}


def trans_time(v_str):
    """Convert GMT time to standard format"""
    GMT_FORMAT = '%a %b %d %H:%M:%S +0800 %Y'
    timeArray = datetime.datetime.strptime(v_str, GMT_FORMAT)
    ret_time = timeArray.strftime("%Y-%m-%d %H:%M:%S")
    return ret_time


def getLongText(v_id):
    """Crawling long Weibo full text"""
    url = 'https://m.weibo.cn/statuses/extend?id=' + str(v_id)
    r = requests.get(url, headers=headers)
    json_data = r.json()
    long_text = json_data['data']['longTextContent']
    # Weibo content - regular expression data cleaning
    dr = re.compile(r'<[^>]+>', re.S)
    long_text2 = dr.sub('', long_text)
    # print(long_text2)
    return long_text2


def get_weibo_list(v_keyword, v_max_page):
    """
	Crawl the content list of Weibo
	:param v_keyword: Search keywords
	:param v_max_page: Crawl the first few pages
	:return: None
	"""

    sum0 = 0
    sum1 = 0
    for page in range(2, v_max_page + 1):


        print('===开始爬取第{}页微博==='.format(page))
        # Request address
        url = 'https://m.weibo.cn/api/container/getIndex'
        # Request parameters
        params = {
            "containerid": "100103type=1&q={}".format(v_keyword),
            "page_type": "searchall",
            "page": page
        }
        # Send request
        r = requests.get(url, headers=headers, params=params)
        print(r.status_code)
        # pprint(r.json())
        # Parse json data
        cards = r.json()["data"]["cards"]
        print(len(cards))
        auid_list=[]
        region_name_list = []
        status_city_list = []
        status_province_list = []
        status_country_list = []
        key = []
        label = []

        for card in cards:
            # Published on
            try:
                key.append("{}".format(v_keyword))

            except:
                key.append('')

            try:

                label.append("0") # Change depression label according to keywords

            except:

                label.append('')

        # Weibo content
        text_list = jsonpath(cards, '$..mblog.text')
        # Weibo content - regular expression data cleaning
        dr = re.compile(r'<[^>]+>', re.S)
        text2_list = []
        print('text_list is:')
        # print(text_list)
        if not text_list:  # If you don't get the Weibo content, enter the next cycle
            continue
        if type(text_list) == list and len(text_list) > 0:
            for text in text_list:
                text2 = dr.sub('', text)  # Regular expression extraction of Weibo content
                # print(text2)
                text2_list.append(text2)

        description_list = jsonpath(cards, '$..mblog.user.description')

        auid_list = jsonpath(cards, '$..mblog.user.id')

        # Weibo creation time
        time_list = jsonpath(cards, '$..mblog.created_at')
        time_list = [trans_time(v_str=i) for i in time_list]
        # Weibo author
        author_list = jsonpath(cards, '$..mblog.user.screen_name')
        # Weibo id
        id_list = jsonpath(cards, '$..mblog.id')
        # Determine whether the full text exists
        isLongText_list = jsonpath(cards, '$..mblog.isLongText')
        idx = 0
        for i in isLongText_list:
            if i == True:
                long_text = getLongText(v_id=id_list[idx])
                text2_list[idx] = long_text
            idx += 1
        # Number of forwards
        reposts_count_list = jsonpath(cards, '$..mblog.reposts_count')
        # Number of comments
        comments_count_list = jsonpath(cards, '$..mblog.comments_count')
        # Number of likes
        attitudes_count_list = jsonpath(cards, '$..mblog.attitudes_count')
        # Save the list data as DataFrame data
        print('id_list:', len(id_list))
        print(len(description_list))
        print(len(time_list))
        print('region_name_list:', len(region_name_list))
        print(len(status_city_list))
        print(len(status_province_list))
        print(len(status_country_list))


        df = pd.DataFrame(
            {

                '微博作者': author_list,
                '作者id': auid_list,
                '爬取自关键词': key,
                '抑郁标签': label,
            }
        )
        # gauge outfit
        if os.path.exists(v_weibo_file):
            header = None
        else:
            header = ['微博作者', '作者id', '爬取自关键词', '抑郁标签']  # Csv file header
        # Save to csv file
        df.to_csv(v_weibo_file, mode='a+', index=False, header=header, encoding='utf_8_sig')

        print('csv保存成功:{}'.format(v_weibo_file))
        # db = pymysql.connect(host='localhost', user='root', password='root', database='test')

        # cursor = db.cursor()


        # if (sum0 < 8000):
        #     for i in range(len(id_list)):
        #         sum0 = sum0 + 1
        #         if (sum0 == 8000):
        #             break
        #
        #         cursor.execute('INSERT INTO USER(NAME,AUID,TIME) VALUES (("%s"), '
        #                        '("%s"), ("%s")) ' % (
        #                            author_list[i], auid_list[i],time_list[i]
        #                             ))
        #
        #         db.commit()
        #         print('插入数据成功')



if __name__ == '__main__':
    engine= create_engine('mysql+pymysql://root:root@localhost/test')

    # Crawl the first few pages
    max_search_page = 100
    # Crawl keywords
    search_keyword = input("请输入话题关键词：")
    # Save File Name
    v_weibo_file = '微博清单_{}_前{}页.csv'.format(search_keyword, max_search_page)
    # If the csv file exists, delete it first
    if os.path.exists(v_weibo_file):
        os.remove(v_weibo_file)
        print('微博清单存在，已删除: {}'.format(v_weibo_file))
    # Call crawling Weibo function
    get_weibo_list(v_keyword=search_keyword, v_max_page=max_search_page)
    # Data cleaning - de-duplication
    df = pd.read_csv(v_weibo_file)
    df.drop_duplicates(inplace=True, keep='first')
    # Save the csv file again
    df.to_csv(v_weibo_file, index=False, encoding='utf_8_sig')

    df2 = pd.read_csv('微博清单_{}_前{}页.csv'.format(search_keyword, max_search_page))
    df2.to_sql(name='user',con=engine, index=False, if_exists='append')
    print('数据清洗完成')
