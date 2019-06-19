# -*- coding: utf-8 -*-
import scrapy
from scrapy.http import Request
import json

from WeatherSpider.items import WeatherItem


class WeatherSpider(scrapy.Spider):
    name = "weather"
    allowed_domains = ["www.weather.com.cn"]

    start_urls = ['http://www.weather.com.cn/']

    # def start_requests(self):
    #     reqs = []
    #     for i in range(1, 16):
    #         if i < 10:
    #             req = scrapy.Request(
    #                 "http://www.weather.com.cn/weather1d/101250%s01.shtml" % i)
    #             reqs.append(req)
    #         else:
    #             if i == 13:
    #                 continue
    #             req = scrapy.Request(
    #                 "http://www.weather.com.cn/weather1d/10125%s01.shtml" % i)
    #             reqs.append(req)
    #     return reqs

    def parse(self, response):
        post_urls = [
            "http://www.weather.com.cn/weather1d/101250101.shtml",
            "http://www.weather.com.cn/weather1d/101250201.shtml",
            "http://www.weather.com.cn/weather1d/101250301.shtml",
            "http://www.weather.com.cn/weather1d/101250401.shtml",
            "http://www.weather.com.cn/weather1d/101250501.shtml",
            "http://www.weather.com.cn/weather1d/101250601.shtml",
            "http://www.weather.com.cn/weather1d/101250701.shtml",
            "http://www.weather.com.cn/weather1d/101250801.shtml",
            "http://www.weather.com.cn/weather1d/101250901.shtml",
            "http://www.weather.com.cn/weather1d/101251001.shtml",
            "http://www.weather.com.cn/weather1d/101251101.shtml",
            "http://www.weather.com.cn/weather1d/101251201.shtml",
            "http://www.weather.com.cn/weather1d/101251401.shtml",
            "http://www.weather.com.cn/weather1d/101251501.shtml"
        ]

        for post_url in post_urls:
            yield Request(url=post_url, callback=self.parse_detail)

    def parse_detail(self, response):

        weather_item = WeatherItem()

        re_selector = response.xpath('//html/body/div[5]/div[1]/div[2]/script/text()').extract()[0]

        weather_json = re_selector[re_selector.find('{'):re_selector.rfind('}') + 1]
        try:
            weather_dict = json.loads(weather_json)
            # print(weather_dict)
            # print(type(weather_dict['od'].get('od1', '城市')))
            # print(weather_dict['od'].get('od1', '城市'))
            weather_item['start_time'] = weather_dict['od'].get('od0', '19700101000000')
            weather_item['area'] = weather_dict['od'].get('od1', 'hn')
            weather_item['data'] = weather_dict['od'].get('od2', [])
            yield weather_item
        except json.JSONDecodeError as e:
            print('json转换出错！待解析数据为：', end=' ')
            print(re_selector)
