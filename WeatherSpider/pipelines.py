# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import MySQLdb


class WeatherSpiderPipeline(object):
    def process_item(self, item, spider):
        return item


class MySqlPipeline(object):
    def __init__(self):
        self.conn = MySQLdb.connect(
            host='carryzheng.xin',
            user='zxxx',
            password='c5AE@lj-jmZU',
            db='test',
            port=3306,
            charset='utf8',
            use_unicode=True
        )
        self.cursor = self.conn.cursor()

    def process_item(self, item, spider):

        start_time = item['start_time']
        area = item['area']
        data = item['data']

        print("start_time:::" + start_time)
        print("area:::" + area)
        print("data:::")
        print(data)

        insert_sql = """
                  insert into weather_spider(qx_id, area, wd, sd, js, fx, fs, fzy, kqzl, create_time, modify_time,qx_date)
                  values('默认id', '北京', null, null, null, '默认风向', null, null, null, 2019111111, 2019111111, 2019111111)
                """
        try:
            self.cursor.execute(insert_sql)
            self.conn.commit()
        except MySQLdb.IntegrityError as e:
            update_sql = """
    
                    """
            self.cursor.execute(update_sql)
            self.conn.commit()

        return item
