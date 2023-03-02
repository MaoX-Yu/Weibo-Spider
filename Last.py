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

if __name__ == '__main__':
    engine= create_engine('mysql+pymysql://root:root@localhost/test')
    print("请输入：")
    x=int(input())
    for i in range(1,x):

        df = pd.read_csv('结果第{}页.csv'.format(i))
        df.to_sql(name='blog',con=engine, index=False, if_exists='append')
        print('数据清洗完成')