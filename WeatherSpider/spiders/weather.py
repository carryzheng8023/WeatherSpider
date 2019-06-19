# -*- coding: utf-8 -*-
import scrapy
import json


class WeatherSpider(scrapy.Spider):
    name = "weather"
    allowed_domains = ["www.weather.com.cn"]

    # start_urls = ['http://www.weather.com.cn/weather1d/101250101.shtml']

    def start_requests(self):
        reqs = []
        for i in range(1, 16):
            if i < 10:
                req = scrapy.Request(
                    "http://www.weather.com.cn/weather1d/101250%s01.shtml" % i)
                reqs.append(req)
            else:
                if i == 13:
                    continue
                req = scrapy.Request(
                    "http://www.weather.com.cn/weather1d/10125%s01.shtml" % i)
                reqs.append(req)
        return reqs

    def parse(self, response):
        re_selector = response.xpath('//html/body/div[5]/div[1]/div[2]/script/text()').extract()[0]
        # print(re_selector)

        weather_json = re_selector[re_selector.find('{'):re_selector.rfind('}') + 1]
        weather_dict = json.loads(weather_json)
        print(weather_dict)

        pass
