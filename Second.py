import time

import pandas as pd

import pymysql
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
from sqlalchemy import create_engine, null

con = pymysql.connect(host='localhost', user='root', password='root', database='test')
df = pd.read_sql(sql="SELECT * from test.user", con=con)
con.close()







# 请求头
headers = {
    "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Mobile Safari/537.36",
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "accept-encoding": "gzip, deflate, br",
}


def trans_time(v_str):
    """转换GMT时间为标准格式"""
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


def get_weibo_list(df0, v_max_page, k):
    """
	Crawl the content list of Weibo
	:param v_keyword: Search keywords
	:param v_max_page: Crawl the first few pages
	:return: None
	"""

    sum0 = 0
    sum1 = 0


    for j in range(k, 14000):
        if j%50==0:
            time.sleep(1)


        url = 'https://m.weibo.cn/api/container/getIndex'
        # 请求参数
        params = {
            "uid": df0['作者id'][j],
            "t": "0",
            "luicode": "10000011",
            "lfid": "100103type=1&q={}".format(df0['微博作者'][j]),
            "type": "uid",
            "value": df0['作者id'][j],
            "containerid": "107603" + str(df0['作者id'][j]),
        }
        # Send request
        r = requests.get(url, headers=headers, params=params)
        print(r.status_code)
        # pprint(r.json())
        # Parse json data
        cards0 = r.json()["data"]["cards"]
        author_list0 = jsonpath(cards0, '$..mblog.user.screen_name')
        description_list0 = jsonpath(cards0, '$..mblog.user.description')
        follower_list = jsonpath(cards0, '$..mblog.user.followers_count')
        follow_count=jsonpath(cards0, '$..mblog.user.follow_count')
        sym_list=jsonpath(cards0, '$..mblog.user.verified_reason')
        acc=jsonpath(cards0, '$..mblog.user.statuses_count')
        gender_list=[]

        for card in cards0:
            # Published on
            try:
                gender = card['mblog']['user']['gender']
                if gender=='m':
                    gender_list.append('男')
                else:
                    gender_list.append('女')

            except:
                gender_list.append('')

        df2 = pd.DataFrame(
            {

                '微博作者': author_list0,
                '性别': gender_list,
                '作者简介': description_list0,
                '关注人数': follow_count,
                '粉丝人数': follower_list,
                '标签': sym_list,
                '推文总数':acc,

            }
        )

        if os.path.exists('result.csv'):
            header = None
        else:
            header = ['微博作者', '性别', '作者简介', '关注人数', '粉丝人数', '标签', '推文总数']  # Csv file header
        # Save to csv file
        df2.drop_duplicates(subset=['微博作者'], inplace=True, keep='first')
        df2.to_csv('result.csv', mode='a+', index=False, header=header, encoding='utf_8_sig')



        for page in range(1, v_max_page + 1):
            print('===开始爬取第{}页微博==='.format(page))
            # Request address
            url = 'https://m.weibo.cn/api/container/getIndex'
            # Request parameters
            params = {
                "uid" : df0['作者id'][j],
                "t" :"0",
                "luicode":"10000011",
                "lfid":"100103type=1&q={}".format(df0['微博作者'][j]),
                "type":"uid",
                "value": df0['作者id'][j],
                "containerid":"107603"+str(df0['作者id'][j]),
                "page": page,
            }
            # 发送请求
            r = requests.get(url, headers=headers, params=params)
            print(r.status_code)
            # print(r.json())
            # Parse json data
            cards = r.json()["data"]["cards"]

            print(len(cards))
            auid_list=[]
            pic_list=[]
            region_name_list = []
            retweet_list = []
            status_city_list = []
            status_province_list = []
            status_country_list = []
            for card in cards:
                # Published on
                try:
                    region_name = card['mblog']['region_name']
                    region_name_list.append(region_name)

                except:
                    region_name_list.append('')

            for card in cards:
                # Published on
                try:

                    retweet = card['mblog']['retweeted_status']['id']
                    retweet_list.append(retweet)
                except:

                    retweet_list.append('')
                # IP dependency_ city
            # Weibo content
            text_list = jsonpath(cards, '$..mblog.text')
            # Weibo content - regular expression data cleaning
            dr = re.compile(r'<[^>]+>', re.S)
            text2_list = []
            print('text_list is:')
            try:
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
                pic_list = jsonpath(cards, '$..mblog.pic_ids')

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

                df1 = pd.DataFrame(
                    {

                        '微博作者': author_list,
                        '作者id': auid_list,
                        '发布时间': time_list,
                        '微博内容': text2_list,
                        '转发数': reposts_count_list,
                        '评论数': comments_count_list,
                        '点赞数': attitudes_count_list,
                        '发布于': region_name_list,
                        '图片' : pic_list,
                        '转发自': retweet_list,

                    }
                )

                # gauge outfit
                if os.path.exists('结果第{}页.csv'.format(j//100+1)):
                    header = None
                else:
                    header = ['推文作者', '作者id', '发布时间', '推文内容', '转发数', '评论数', '点赞数', '发布于','图片','转发自' ]  # Csv file header
                # Save to csv file
                df1.to_csv('结果第{}页.csv'.format(j//100+1), mode='a+', index=False, header=header, encoding='utf_8_sig')



                print('csv保存成功')
            except:
             continue
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
    x=0
    engine= create_engine('mysql+pymysql://root:root@localhost/test')
    print("请输入：")
    x=int(input())


    # Crawl the first few pages
    max_search_page = 20
    # Call crawling Weibo function
    get_weibo_list(df, v_max_page=max_search_page,k=x)
    # Data cleaning - de-duplication
    sql_cmd = "SELECT * FROM user"
    df = pd.read_sql(sql=sql_cmd, con=engine)
    df2 = pd.read_csv('result.csv')
    df2.to_sql(name='user_info', con=engine, index=False, if_exists='append')



    print('数据清洗完成')