# -*- coding: utf-8 -*-
import pymysql

from __config import *
from log.data_log import Logger


class BaseClient(object):
    def __init__(self, entity_code=None, **kwargs):
        if not kwargs:
            self.mysql_host = MYSQL_HOST
            self.mysql_port = MYSQL_PORT
            self.mysql_database = MYSQL_DATABASE
            self.mysql_table = MYSQL_TABLE
            self.mysql_user = MYSQL_USER
            self.mysql_password = MYSQL_PASSWORD

        else:
            self.mysql_host = kwargs["host"]
            self.mysql_port = kwargs["port"]
            self.mysql_database = kwargs["database"]
            self.mysql_user = kwargs["user"]
            self.mysql_password = kwargs["password"]
            try:
                self.mysql_table = kwargs["table"]
            except KeyError:
                self.mysql_table = MYSQL_TABLE

        self.mysql_entity_code = entity_code

        self.mysql_config = {
            "host": self.mysql_host,
            "port": self.mysql_port,
            "database": self.mysql_database,
            "user": self.mysql_user,
            "password": self.mysql_password,
            "charset": "utf8"
        }

    def client_to_mysql(self):
        mysql_logger = Logger().logger
        try:
            mysql_logger.info("正在连接MySQL({}@{}:{})".format(self.mysql_user, self.mysql_host, self.mysql_port))
            connection = pymysql.connect(**self.mysql_config)
            mysql_logger.info("Mysql连接成功({}@{}:{})".format(self.mysql_user, self.mysql_host, self.mysql_port))
            return connection
        except pymysql.err.OperationalError as e:
            for retry_count in range(2, 7):
                try:
                    mysql_logger.warning("MySQL连接失败，正在重试第{}次连接".format(retry_count))
                    connection = pymysql.connect(**self.mysql_config)
                    mysql_logger.info("Mysql连接成功")
                    return connection
                except Exception as e:
                    mysql_logger.warning("第{}次连接MySQL失败".format(retry_count))
                    # print(retry_count)
                    if retry_count == 6:
                        mysql_logger.error("MySQL连接失败,错误信息为{}".format(e))

    @staticmethod
    def cs_commit(connection, sql):
        # print(sql)
        cs = connection.cursor()
        count = cs.execute(sql)
        connection.commit()
        cs.close()
        return count


class MysqlClient(BaseClient):
    def insert_to_mysql(self, connection, data):
        """
        插入新数据
        :param connection:
        :param data: type => tuple List or dict
        :return:
        """
        mysql_logger = Logger().logger

        if isinstance(data, dict):
            k_list = [key for key in data.keys()]
            v_list = tuple([value for value in data.values()])
            v_sql = str(v_list)
            if v_sql[-2] == ",":
                v_sql = v_sql[:-2] + ")"
            sql = f"INSERT INTO {self.mysql_table} ({','.join(k_list)}) VALUES{v_sql}"
        elif isinstance(data, (list, tuple)):
            k_list = [key for key in data[0].keys()]
            value_list = list()
            for each in data:
                v_list = str(tuple([value for value in each.values()]))
                if v_list[-2] == ",":
                    v_list = v_list[:-2] + ")"
                value_list.append(v_list)
            sql = f"INSERT INTO {self.mysql_table} ({','.join(k_list)}) VALUES"
            sql = sql + ",".join(value_list)
        else:
            raise Exception("not format type of data")
        try:
            mysql_logger.info(f"sql==>{sql}")
            count = self.cs_commit(connection=connection, sql=sql)
            mysql_logger.info(f"MySQL 插入成功 {count} 条")
        except Exception as e:
            mysql_logger.exception(f"插入失败，ERROR: {e}")

    def delete_from_mysql(self, connection, where_condition):
        """
        删除
        :param connection:
        :param where_condition: where 条件
        :return:
        """
        mysql_logger = Logger().logger

        if "where" in where_condition or "WHERE" in where_condition:
            sql = f"DELETE FROM {self.mysql_table} {where_condition}"
        else:
            sql = f"DELETE FROM {self.mysql_table} WHERE {where_condition}"

        try:
            count = self.cs_commit(connection=connection, sql=sql)
            mysql_logger.info(f"MySQL 删除成功 {count} 条")
        except Exception as e:
            mysql_logger.exception(f"MySQL 删除失败，ERROR: {e}")

    def update_to_mysql(self, connection, data, where_condition):
        """
        更新数据
        :param connection:
        :param data:
        :param where_condition: where 条件
        :return:
        """
        mysql_logger = Logger().logger

        set_list = list()
        for key, value in data.items():
            set_list.append(f"{key} = \'{value}\'")

        if "where" in where_condition or "WHERE" in where_condition:
            sql = f"UPDATE {self.mysql_table} SET {','.join(set_list)} {where_condition}"
        else:
            sql = f"UPDATE {self.mysql_table} SET {','.join(set_list)} WHERE {where_condition}"

        try:
            count = self.cs_commit(connection=connection, sql=sql)
            mysql_logger.info(f"MySQL 更新成功 {count} 条")
        except Exception as e:
            mysql_logger.exception(f"MySQL 更新失败，ERROR: {e}")

    def search_from_mysql(self, connection, output=None, where_condition=None, limit_num=None, offset_num=None):
        """
        查询
        :param connection:
        :param output: 输出字段
        :param where_condition: where 条件
        :param limit_num: 输出数量
        :param offset_num: 跳过数量
        :return:
        """
        mysql_logger = Logger().logger
        if output:
            if isinstance(output, str):
                sql = f"SELECT {output} FROM {self.mysql_table}"
            elif isinstance(output, (tuple, list)):
                sql = f"SELECT {','.join(output)} FROM {self.mysql_table}"
            else:
                raise Exception("not format type of \"output\"")
        else:
            sql = f"SELECT * FROM {self.mysql_table}"

        if where_condition:
            if "where" in where_condition or "WHERE" in where_condition:
                sql = sql + " " + where_condition
            else:
                sql = sql + f" WHERE {where_condition}"

        sql = sql + f" LIMIT {limit_num}" if limit_num else sql

        sql = sql + f" OFFSET {offset_num}" if offset_num else sql
        # print(sql)
        try:
            cs = connection.cursor(pymysql.cursors.DictCursor)
            count = cs.execute(sql)
            result = cs.fetchall()
            if count:
                mysql_logger.info(f"Mysql 查取成功 {count} 条")
                return result
            else:
                mysql_logger.info("数据库查取数为0")
        except TypeError:
            mysql_logger.error("MySQL查取失败，请检查")
        finally:
            cs.close()

    # 根据id删除数据
    def remove_from_mysql(self, connection, list_remove):
        mysql_logger = Logger().logger
        sql = "DELETE FROM {} WHERE ID_ IN {}".format(self.mysql_table, list_remove)
        count = self.cs_commit(connection=connection, sql=sql)
        mysql_logger.info("删除成功 {}条".format(count))
        # self.close_client(connection=connection)

    def close_client(self, connection):
        connection.close()

    def main(self):
        connection = self.client_to_mysql()
        result = self.search_from_mysql(connection)
        # print(result)
        return result


if __name__ == '__main__':
    config = {
        "host": "localhost",
        "port": 3306,
        "database": "area",
        "user": "root",
        "password": "mysql",
        "table": "fin_test"
    }
    start = MysqlClient(**config)
    # start.main()
    connection = start.client_to_mysql()
    result = start.search_from_mysql(connection=connection,where_condition=" ENTITY_ID_='WECHAT0000084001'")
    print(result)
