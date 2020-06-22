# coding=utf-8
import datetime

import requests
from lxml import etree
from scrapy.selector import Selector


class FundFlow(object):
    # 同花顺数据中心-资金流数据采集

    def __init__(self):
        self.date = None                # 日期
        self.header = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.97 Safari/537.36',
            'Referer': 'http://data.10jqka.com.cn/funds/gnzjl/',
            'X-Requested-With': 'XMLHttpRequest',
            'hexin-v': 'AmZZzTo6BCHKUtCF6bModSfFt9frR6oNfIveZVAPUglk0whBuNf6EUwbLncj',
            'Host': 'data.10jqka.com.cn'
        }

    def get_theme_fund_flow(self):
        """ 获取概念、行业资金流 """

        # 获取概念资金流
        page_max = 7  # 最大页数
        for page_num in range(1, page_max+1):
            page_url = ('http://data.10jqka.com.cn/funds/gnzjl/field/'
                        'tradezdf/order/desc/page/{0}/ajax/1/'.format(page_num))
            page_url = 'http://data.10jqka.com.cn/funds/gnzjl/field/tradezdf/order/desc/ajax/{0}/free/1/'.format(page_num)
            response = requests.get(page_url, headers=self.header)
            self.extract_theme_page(response.text, fund_type=1)
        print('Finish-获取概念资金流')
        # 获取行业资金流
        page_max = 2  # 最大页数
        for page_num in range(1, page_max+1):
            page_url = ('http://data.10jqka.com.cn/funds/hyzjl/field/'
                        'tradezdf/order/desc/page/{0}/ajax/1/'.format(page_num))
            response = requests.get(page_url)
            self.extract_theme_page(response.text, fund_type=2)
        print('Finish-获取行业资金流')

    def extract_theme_page(self, text, fund_type):
        """ 提取概念/行业页面内容，并保存
        :param text: response.text
        :param fund_type: 1表示概念，2表示行业
        :return: None
        """
        html = Selector(text=text)
        elements = html.xpath('//table[@class="m-table J-ajax-table"]/tbody/tr')
        # print(elements)
        # print(type(elements))
        # exit()
        for element in elements:
            name = element.xpath('.//td[2]/a/text()').extract()[0]  # 概念名称
            index = float(element.xpath('.//td[3]/text()').extract()[0])  # 行业指数
            rose_ratio = float(element.xpath('.//td[4]/text()').extract()[0].rstrip('%'))  # 涨跌幅
            fund_amount_in = float(element.xpath('.//td[5]/text()').extract()[0])   # 流入资金
            fund_amount_out = float(element.xpath('.//td[6]/text()').extract()[0])  # 流出资金
            fund_real_in = float(element.xpath('.//td[7]/text()').extract()[0])  # 净额
            company_num = int(element.xpath('.//td[8]/text()').extract()[0])  # 公司家数
            leader = element.xpath('.//td[9]/a/text()').extract()[0]  # 领涨股
            leader_rose_ratio = float(element.xpath('.//td[10]/text()').extract()[0].rstrip('%'))  # 涨跌幅
            leader_price = float(element.xpath('.//td[11]/text()').extract()[0])  # 当前价
            print(name + "---" + str(fund_amount_in))

    def get_stock_fund_flow(self):
        """ 获取个股资金流 """
        page_max = 63  # 最大页数
        for page_num in range(1, page_max+1):
            page_url = 'http://data.10jqka.com.cnfunds/ggzjl/field/zdf/order/desc/page/{0}/ajax/1/'.format(page_num)
            response = requests.get(page_url)
            self.extract_stock_page(response.text)
        # self.db_session.commit()
        print('Finish-获取个股资金流')

    def extract_stock_page(self, text):
        """ 提取个股页面内容, 并保存
        :param text: response.text
        :return: None
        """
        selector = etree.HTML(text)
        elements = selector.xpath('//table[@class="m-table J-ajax-table"]/tbody/tr')
        for element in elements:
            symbol = element.xpath('.//td[2]/a/text()')[0]                              # 代码
            name = element.xpath('.//td[3]/a/text()')[0]                                # 名称
            price = float(element.xpath('.//td[4]/text()')[0])                          # 最新价格
            rose_ratio = float(element.xpath('.//td[5]/text()')[0].rstrip('%'))         # 涨跌幅
            hand_ratio = float(element.xpath('.//td[6]/text()')[0].rstrip('%'))         # 换手率
            fund_amount_in = self.handle_data(element.xpath('.//td[7]/text()')[0])      # 流入资金
            fund_amount_out = self.handle_data(element.xpath('.//td[8]/text()')[0])     # 流出资金
            fund_real_in = self.handle_data(element.xpath('.//td[9]/text()')[0])        # 净额
            trade_amount = self.handle_data(element.xpath('.//td[10]/text()')[0])       # 成交额
            big_trade_in = self.handle_data(element.xpath('.//td[11]/text()')[0])       # 大单流入

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
        return float(text)

    def run(self):
        """ 触发函数 """
        # try:
        #     print('提示-启动同花顺资金流爬虫')
        #     self.get_theme_fund_flow()
        #     # self.get_stock_fund_flow()
        #     print('提示-完成资金流数据获取：{0}的获取'.format(self.date))
        # except Exception as e:
        #     print(e)
        self.get_theme_fund_flow()


if __name__ == '__main__':
    host = FundFlow()
    host.run()