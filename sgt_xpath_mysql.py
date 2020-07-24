# coding=utf-8

import requests
from scrapy.selector import Selector
from useragent import agent_list
import random
import time
from datetime import datetime
from _mysql import MysqlClient


class FundFlow(object):
    # 同花顺数据中心-资金流数据采集

    def __init__(self):
        self.date = None                # 日期
        self.header = {
            'User-Agent': random.choice(agent_list),
            'Referer': 'http://data.10jqka.com.cn/hgt/sgtb/',
            'X-Requested-With': 'XMLHttpRequest',
            'hexin-v': 'ApQZ-bnhtojX9CJEXBh6g-GPZdkF7bkr-hNMAC51ISqjwDrPVv2IZ0ohHKh9',
            'Host': 'data.10jqka.com.cn'

        }
        self.mysql_client = MysqlClient()
        self.mysql_connect = self.mysql_client.client_to_mysql()

    def get_data_from_mysql(self, where_condition):
        return self.mysql_client.search_from_mysql(connection=self.mysql_connect, where_condition=where_condition)

    def get_sgt_fund_flow(self):
        """ 获取深股通资金流 """

        # 获取概念资金流
        page_max = 10  # 最大页数
        for page_num in range(1, page_max+1):
            print("正在爬取第{page}页".format(page=page_num))
            page_url = 'http://data.10jqka.com.cn/hgt/sgtb/field/jlr/order/desc/ajax/1/page/{page_num}/'.format(page_num=page_num)
            response = requests.get(page_url, headers=self.header)

            self.extract_sgt_page(response.text)

    def extract_sgt_page(self, text):
        """ 提取深股通页面内容，并保存
        :param text: response.text
        :return: None
        """
        html = Selector(text=text)
        elements = html.xpath('//div[@id="table2"]/table/tbody/tr')

        for element in elements:
            sgt_data = dict()
            sgt_data["stock_code"] = element.xpath('.//td[2]/a/text()').extract()[0]  # 股票代码
            sgt_data["stock_name"] = element.xpath('.//td[3]/a/text()').extract()[0]  # 股票名称
            sgt_data["price_new"] = element.xpath('.//td[4]/text()').extract()[0]   # 最新价
            sgt_data["low_to_high"] = element.xpath('.//td[6]/text()').extract()[0]   # 涨跌幅
            sgt_data["last_price"] = element.xpath('.//td[7]/text()').extract()[0]   # 昨收
            sgt_data["price_open"] = element.xpath('.//td[8]/text()').extract()[0]   # 今开
            sgt_data["price_high"] = element.xpath('.//td[9]/text()').extract()[0]   # 最高
            sgt_data["price_low"] = element.xpath('.//td[10]/text()').extract()[0]   # 最低
            sgt_data["fund_aount_in"] = str(self.handle_data(element.xpath('.//td[11]/text()').extract()[0]))  # 资金净流入
            sgt_data["CJE"] = str(self.handle_data(element.xpath('.//td[13]/text()').extract()[0]))  # 成交额
            sgt_data["HS"] = element.xpath('.//td[14]/text()').extract()[0]  # 换手率
            sgt_data["trade_date"] = time.strftime("%Y%m%d", time.localtime())
            print(sgt_data)
            where_condition = f'WHERE stock_name ="{sgt_data["stock_name"]}" and trade_date="{sgt_data["trade_date"]}"'
            # print(self.get_data_from_mysql(where_condition=where_condition))
            if not self.get_data_from_mysql(where_condition=where_condition):
                self.mysql_client.insert_to_mysql(connection=self.mysql_connect, data=sgt_data)
            else:
                print("数据重复,重复值为{0}".format(sgt_data["stock_name"]))
        time.sleep(5)

    @staticmethod
    def if_weekend(day_str, separator=""):
        """
        if a day is weekend
        :param day_str: string of a day
        :param separator: separator of year, month and day, default is empty
        :return: True: is weekend; False: not weekend
        """
        spec = "%Y" + separator + "%m" + separator + "%d"
        day = datetime.strptime(day_str, spec).date()
        # Monday == 0 ... Sunday == 6
        if day.weekday() in [5, 6]:
            return True
        else:
            return False

    @staticmethod
    def handle_data(text):
        """ 统一个股页面单位表示，全部转为万
        :param text: 字符串
        :return: float
        """
        text = text.strip()
        if '亿' in text:
            text = text.rstrip('亿')
            text = float(text) * 10000
        elif '万' in text:
            text = text.rstrip('万')
        return round(float(text),2)

    def run(self):
        """ 触发函数 """
        # try:
        #     print('提示-启动同花顺资金流爬虫')
        #     self.get_theme_fund_flow()
        #     # self.get_stock_fund_flow()
        #     print('提示-完成资金流数据获取：{0}的获取'.format(self.date))
        # except Exception as e:
        #     print(e)
        datestr = time.strftime("%Y%m%d", time.localtime())
        if not self.if_weekend(datestr):
            self.get_sgt_fund_flow()
        # where_condition = f'WHERE stock_name = "同花顺" and trade_date="20200610"'
        # print(self.get_data_from_mysql(where_condition=where_condition))


if __name__ == '__main__':
    host = FundFlow()
    host.run()
