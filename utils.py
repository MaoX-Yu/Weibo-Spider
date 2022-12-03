# @File: utils.py
# @Author: MaoX-Yu
# @Time: 2022/12/2 17:45
# @Desc: 微博爬虫中需要用到的工具
# @Update: 2022/12/3 17:15
import os
import re
from datetime import datetime


def check_dir(path_name):
    """ 检查路径是否存在，并创建不存在的目录

    :param path_name: 路径名
    :return: 无返回值
    """
    if not os.path.exists(path_name):
        os.mkdir(path_name)


def next_page(url):
    """ 返回下一页的url

    :param url: 网址
    :return: url
    """
    try:
        page = int(re.findall(r"page=(?P<page>\d+)", url).pop())
    except IndexError:
        page = 1
    page = str(page + 1)
    url_split = url.split('&')
    url = url_split[0] + '&page=' + page
    return url


def insert_sql(table, value_list, logger=None):
    """ 生成插入值的sql语句

    :param table: 表名
    :param value_list: 插入值列表
    :param logger: 日志记录器
    :return: sql语句
    """
    sql = "INSERT INTO {0} VALUES ({1});"
    value = ''

    for index, item in enumerate(value_list):
        if not isinstance(item, str):  # 类型不是字符串
            value += str(item)
        elif item == '' or len(item) == 0:  # 值为空
            value += 'null'
        else:
            value += "'" + item + "'"

        if index != len(value_list) - 1:
            value += ','

    sql = sql.format(table, value)
    if logger is not None:
        logger.info(sql)
    return sql


def rm_html(text):
    """ 去除html标签

    :param text: 源文本
    :return: 去除html标签后的文本
    """
    re_comp = re.compile(r'<.+?>')
    result = re_comp.sub('', text)
    return result


def standardize_date(created_at):
    """ 格式化微博发布时间

    :param created_at: 源时间信息
    :return: 格式化后的时间信息
    """
    result = datetime.strptime(created_at, "%a %b %d %H:%M:%S +0800 %Y")
    return result
