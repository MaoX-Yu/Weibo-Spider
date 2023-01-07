# 微博爬虫

> 环境: Python3.10

---

### 功能

爬取在某一话题中发布过内容的用户的个人信息和历史博文

### 配置说明

> 配置文件: `config.json`

`headers`: 请求头

- `User-Agent`
- `cookie`

`user_num`: 需要获取的用户数量

`blog_num`: 每位用户获取的博文数量

`sleep_time`: 休眠时间

- `min`
- `max`

`mysql`: mysql数据库配置

- `host`
- `user`
- `password`
- `port`
- `db_name`
- `charset`

`download_pic`: 是否下载图片

### 使用方法

1. `pip install -r requirement.txt`
2. 重命名文件`config_example.json`为`config.json`
3. 运行`weibo_spider.py`
