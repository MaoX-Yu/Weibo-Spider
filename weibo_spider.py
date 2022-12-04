# @File: weibo_spider.py
# @Author: MaoX-Yu
# @Time: 2022/9/28 16:32
# @Desc: 微博爬虫
# @Update: 2022/12/4 11:15
import re
import json
import random
import traceback
from time import sleep
import logging
from logging import handlers

import pandas
import pymysql
import requests
from tqdm import tqdm

from utils import (
    check_dir,
    rm_html,
    insert_sql,
    standardize_date,
    next_page
)


class WeiboSpider:
    def __init__(self):
        """ 初始化 """
        self.db = None
        self.config = None

        check_dir('images')
        check_dir('temp')
        self.logger = logging.getLogger()
        self.set_logger()

        with open('uid.csv', 'w', encoding='utf-8') as f:  # 清空uid.csv文件
            f.write('')

        try:
            with open("config.json", mode="r", encoding="utf-8") as f:  # 获取配置
                self.config = json.load(f)
        except FileNotFoundError:
            self.logger.error('config.json文件未找到')
            exit(1)

        try:
            self.headers = self.config['headers']
            self.mysql = self.config['mysql']
            self.user_num = self.config['user_num']
            self.blog_num = self.config['blog_num']
            self.sleep_time_range = self.config['sleep_time']
        except Exception:
            self.logger.error('配置文件有误，错误信息：{0}'.format(traceback.format_exc(limit=1)))
            exit(1)

        self.connect_db()

        if 'cookie' not in self.headers or self.headers['cookie'] == '' or len(self.headers['cookie']) == 0:
            self.logger.error("请先在config.json中配置cookie")
            exit(1)

    def set_logger(self):
        """ 配置日志记录器 """
        self.logger.setLevel(level=logging.DEBUG)

        formatter = logging.Formatter('%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')

        file_handler = handlers.TimedRotatingFileHandler(filename='temp/weibo_spider.log', when='D', encoding='utf-8')
        file_handler.setLevel(level=logging.INFO)
        file_handler.setFormatter(formatter)

        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.WARNING)
        stream_handler.setFormatter(formatter)

        self.logger.addHandler(file_handler)
        self.logger.addHandler(stream_handler)

    def connect_db(self):
        """ 连接mysql数据库 """
        try:
            self.db = pymysql.connect(host=self.mysql['host'],  # 连接名称，默认127.0.0.1
                                      user=self.mysql['user'],  # 用户名
                                      passwd=self.mysql['password'],  # 密码
                                      port=self.mysql['port'],  # 端口，默认为3306
                                      db=self.mysql['db_name'],  # 数据库名称
                                      charset=self.mysql['charset'],  # 字符编码
                                      )
        except Exception:
            self.logger.error("数据库连接失败，错误信息：{0}".format(traceback.format_exc(limit=1)))
            exit(1)

    def sleep_time(self):
        """ 随机睡眠一定时间 """
        num = random.randint(self.sleep_time_range['min'], self.sleep_time_range['max'])
        sleep(num)

    def get_real_url(self, search_key):
        """ 获取话题对应的url

        :param search_key: 话题关键词
        :return: url
        """
        url = "https://s.weibo.com"
        search_key = '#' + search_key + '#'
        params = {
            "q": search_key,
        }

        re_comp = re.compile(
            r'''<head>.*?<script type="text/javascript">.*?CONFIG\['s_search'] = '(?P<search>.*?)'.*?</script>.*?</head>''',
            re.S)

        resp = requests.get(url, params=params, headers=self.headers)
        resp.encoding = "utf-8"
        result = re_comp.findall(resp.text)
        url_real = str.format("{0}/weibo?q={1}", url, result.pop())
        resp.close()
        return url_real

    def get_html(self, url):
        """ 获取网页的html

        :param url: 网址
        :return: html
        """
        resp = requests.get(url, headers=self.headers)
        resp.encoding = "utf-8"
        result = resp.text
        resp.close()
        return result

    def get_details(self, uid, uname):
        """ 获取用户的详细信息

        :param uid: 用户uid
        :param uname: 用户名
        :return: 无返回值
        """
        details = [str(uid), uname]
        url_info = 'https://weibo.com/ajax/profile/info'
        url = 'https://weibo.com/ajax/profile/detail'
        params = {
            'uid': uid
        }

        resp = requests.get(url_info, headers=self.headers, params=params)
        resp.encoding = 'utf-8'
        info = resp.json()['data']['user']
        resp.close()

        if info['verified'] and 0 < info['verified_type'] < 200:  # 判断是否为官方号
            self.logger.info('用户ID:{0} 用户名:{1} 不符合条件'.format(uid, uname))
            return

        for item in ['followers_count', 'location']:
            if item in info:
                details.append(info[item])
            else:
                details.append('')

        resp = requests.get(url, headers=self.headers, params=params)
        resp.encoding = "utf-8"
        data = resp.json()['data']
        resp.close()

        for key in ['birthday', 'desc_text']:
            if key in data:
                if key == 'birthday':  # 处理生日，去掉星座
                    t = re.search(r'\d{4}-\d{2}-\d{2}', data[key])
                    if t:
                        data[key] = t.group()
                    else:
                        data[key] = ''
                details.append(data[key])
            else:
                details.append('')

        # self.logger.info(details)
        self.db_insert('user', details)

    def get_blogs(self, uid, count):
        """ 获取用户的历史博文

        :param uid: 用户uid
        :param count: 博文数量
        :return:
        """
        page = 1
        blog_count = 0
        since_id = ''
        pic_list = []
        blog_msg = []
        blog_key = ['idstr', 'text', 'attitudes_count', 'pic_ids', 'created_at']  # 博文id，博文内容，点赞，图片id
        is_long_text = False
        long_text = ''
        url = 'https://weibo.com/ajax/statuses/mymblog'

        try:
            with tqdm(total=count, desc='正在获取用户{0}的博文'.format(uid)) as bar:
                while blog_count < count:
                    if page == 1:
                        params = {
                            'uid': uid,
                            'page': 1,
                            'feature': 0
                        }
                    else:
                        params = {
                            'uid': uid,
                            'page': page,
                            'feature': 0,
                            'since_id': since_id
                        }

                    resp = requests.get(url, headers=self.headers, params=params)
                    resp.encoding = 'utf-8'
                    data = resp.json()['data']
                    blog_list = data['list']
                    since_id = data['since_id'] if 'since_id' in data else ''
                    resp.close()

                    for i in range(len(blog_list)):
                        if blog_count >= count:
                            return
                        elif 'retweeted_status' in blog_list[i]:
                            break
                        elif 'page_info' in blog_list[i]:
                            break
                        elif 'title' in blog_list[i] and '赞过的微博' in blog_list[i]['title']['text']:
                            break
                        elif blog_list[i]['isLongText']:
                            try:
                                long_text = self.get_long_blog(blog_list[i]['mblogid'])
                                is_long_text = True
                            except Exception:
                                is_long_text = False

                        for key in blog_key:
                            if key in blog_list[i]:
                                if key == 'pic_ids':
                                    pic_list = blog_list[i][key]
                                elif key == 'text':
                                    if is_long_text:
                                        blog_msg.append(rm_html(long_text))
                                    else:
                                        blog_msg.append(rm_html(blog_list[i][key]))
                                elif key == 'created_at':
                                    blog_msg.append(str(standardize_date(blog_list[i][key])))
                                else:
                                    blog_msg.append(blog_list[i][key])

                        blog_msg.append(str(uid))
                        self.db_insert('blog', blog_msg)

                        for index, pic in enumerate(pic_list):
                            if self.db_insert('picture', [pic, blog_list[i]['idstr']]):
                                self.download_pic(pic)

                            if (index+1) % 3 == 0:
                                self.sleep_time()

                        blog_msg = []
                        pic_list = []
                        blog_count += 1
                        is_long_text = False
                        bar.update(1)

                    page += 1
                    self.sleep_time()
        except Exception:
            self.logger.error(traceback.format_exc(limit=1))

    def get_long_blog(self, blog_id):
        """ 返回长微博的博文

        :param blog_id:
        :return:
        """
        url = 'https://weibo.com/ajax/statuses/longtext'
        params = {
            "id": blog_id
        }

        resp = requests.get(url, headers=self.headers, params=params)
        result = resp.json()['data']['longTextContent']
        resp.close()
        return result

    def get_uid(self, url, count):
        """ 从话题中获取用户的uid和用户名

        :param url: 话题url
        :param count: 获取用户数量
        :return: 无返回值
        """
        user_count = 0
        re_comp = re.compile(
            r'''<div style="padding: 6px 0 3px;">.*?weibo.com/(?P<uid>\d+)\?.*?nick-name="(?P<username>.*?)".*?</div>''',
            re.S)

        with tqdm(total=count, desc='正在获取用户ID') as bar:
            while user_count < count:
                str_html = self.get_html(url)
                iter_ = re_comp.finditer(str_html)

                with open("uid.csv", mode="a", encoding="utf-8") as f:
                    for i in iter_:
                        if user_count >= count:
                            break
                        uid = i.group("uid")
                        username = i.group("username")
                        f.write(uid + ',' + username)
                        f.write('\n')
                        user_count += 1
                        bar.update(1)

                url = next_page(url)
                self.sleep_time()

    def db_insert(self, table, value):
        """ 将数据写入数据库

        :param table: 表名
        :param value: 插入值
        :return: bool
        """
        flag = True
        cur = self.db.cursor()
        try:
            cur.execute(insert_sql(table, value, self.logger))
            self.db.commit()
        except Exception:
            self.logger.warning("ID:{0} 记录插入失败，错误信息:{1}".format(value[0], traceback.format_exc(limit=1)))
            self.db.rollback()
            flag = False
        finally:
            cur.close()
            return flag

    def download_pic(self, pic_id):
        """ 下载图片

        :param pic_id: 图片id
        :return: 无返回值
        """
        url = 'https://weibo.com/ajax/common/download'
        params = {
            'pid': pic_id
        }

        resp = requests.get(url, params=params, headers=self.headers)
        with open('images/' + str(pic_id) + '.jpg', 'wb') as f:
            f.write(resp.content)
        resp.close()

    def run(self):
        try:
            print("微博爬虫")
            print("config.json为配置文件")
            print("------------------------")
            search_key = input("请输入话题关键词：")
            url_real = self.get_real_url(search_key)

            print("------ 获取用户UID ------")
            print("共获取{0}位用户".format(self.user_num))
            sleep(0.5)
            self.get_uid(url_real, self.user_num)
            sleep(0.5)

            print("------ 获取用户详细信息 ------")
            sleep(0.5)
            id_df = pandas.read_csv('uid.csv', names=['uid', 'username'])
            for i in tqdm(range(self.user_num)):
                self.get_details(id_df.iloc[i]['uid'], id_df.iloc[i]['username'])
                if (i + 1) % 5 == 0:  # 每爬取一定个数的信息后进行休眠
                    self.sleep_time()
            sleep(0.5)

            print("------ 获取用户历史推文 ------")
            print("每位用户获取{0}条博文".format(self.blog_num))
            sleep(0.5)
            for i in range(self.user_num):
                self.get_blogs(id_df.iloc[i]['uid'], self.blog_num)
                if (i + 1) % 5 == 0:  # 每爬取一定个数的信息后进行休眠
                    self.sleep_time()
        except Exception:
            self.logger.error(traceback.format_exc(limit=1))
        finally:
            self.db.close()


if __name__ == '__main__':
    app = WeiboSpider()
    app.run()
