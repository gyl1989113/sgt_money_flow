# -*- coding: utf-8 -*-
"""配置文件"""

"MongoDB 默认配置"
# MongoDB 地址
MONGO_HOST_35 = ["172.22.69.35:20000"]
MONGO_HOST_LH = ["localhost:27017"]
MONGO_HOST_25 = ["172.22.67.25:27017"]
MONGO_HOST_41 = ["172.22.69.41:20000"]
MONGO_PRO = ["192.168.1.73:27017", "192.168.1.81:27017", "192.168.1.102:27017"]

# MongoDB 数据库(str)
MONGO_DB = "spider_data"
# MONGO_DB = "spider_data_old"



"MySQL 线上配置"
# MySQL 地址(str)
MYSQL_HOST = "192.168.43.20"
# MySQL 端口(int)
MYSQL_PORT = 3306
# MySQL 数据库(str)
MYSQL_DATABASE = "sgt_data"
# # MySQL 表名(str)
MYSQL_TABLE = "sgt_fund_flow"
# MySQL 用户名
MYSQL_USER = "root"
# MySQL 密码
MYSQL_PASSWORD = "root001"




# 环境
ENV = "pro"  # 生产环境
# ENV = "dev"  # 测试环境

TABLE_NAME = lambda name: name if ENV == "pro" else "TEST_" + name
