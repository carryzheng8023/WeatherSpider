# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import MySQLdb
import datetime
import uuid
from WeatherSpider.utils.common import parse_float
from WeatherSpider.utils.common import parse_int
from WeatherSpider.utils.common import fl_dict
from WeatherSpider.utils.common import area_dict
from WeatherSpider.utils.common import area_num_dict
from twisted.enterprise import adbapi
import MySQLdb.cursors


class WeatherSpiderPipeline(object):
    def process_item(self, item, spider):
        return item


class MySqlPipeline(object):
    def __init__(self):

        self.conn = MySQLdb.connect(
            host='10.6.6.127',
            user='root',
            password='ausadmin123',
            db='csust',
            port=3307,
            charset='utf8',
            use_unicode=True
        )
        self.cursor = self.conn.cursor()

    def process_item(self, item, spider):

        start_time = item['start_time']
        area_name = item['area']
        data = item['data']

        # print("start_time:::" + start_time)
        # print("area_name:::" + area_name)
        # print("data:::")
        # print(data)

        now = datetime.datetime.now()
        create_time = now.strftime('%Y%m%d%H%M%S')
        modify_time = now.strftime('%Y%m%d%H%M%S')

        for weather in reversed(data):

            is_error = 0
            try:
                qx_id = start_time[:10] + area_num_dict[area_name] + weather['od21']
            except Exception as e:
                print(e)
                is_error = 1
                qx_id = uuid.uuid1().get_hex()[16:]

            # print("qx_id:::" + qx_id)
            area = area_dict.get(area_name, '')
            wd = parse_float(weather.get('od22', 0.0))
            sd = parse_float(weather.get('od27', 0.0))
            js = parse_float(weather.get('od26', 0.0))
            fx = weather.get('od24', '暂无风向')
            fl = parse_int(weather.get('od25', 0.0))
            fs = fl_dict.get(fl, 0.0)
            fzy = 0.58 * fs * fs * fs
            fzy = 0.1 if fzy < 0.1 else fzy
            kqzl = parse_float(weather.get('od28', 0.0))

            qx_date = start_time[:10]

            insert_sql = """
                      insert into dm_qx_weather_spider(qx_id, area, wd, sd, js, fx, fl, fs, fzy, kqzl, create_time, 
                      modify_time, qx_date, is_error)
                      values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
            # print("insert_sql:::" + insert_sql)
            try:
                self.cursor.execute(insert_sql, (
                    qx_id, area, wd, sd, js, fx, fl, fs, fzy, kqzl, create_time, modify_time, qx_date, is_error))
                self.conn.commit()
                # print("插入" + qx_id + "完成！")
            except Exception as e:
                print(e)
                update_sql = """
                          update dm_qx_weather_spider set wd = %s, sd = %s, js = %s, fl = %s, fs = %s, fzy = %s, kqzl = %s, 
                          modify_time = %s where qx_id = %s
                        """
                # print("update_sql:::" + update_sql)
                self.cursor.execute(update_sql, (wd, sd, js, fl, fs, fzy, kqzl, modify_time, qx_id))
                self.conn.commit()
                # print("更新" + qx_id + "完成！")
            s_t = datetime.datetime.strptime(start_time, '%Y%m%d%H%M%S')
            start_time = (s_t + datetime.timedelta(hours=1)).strftime('%Y%m%d%H%M%S')

        return item


class MysqlTwistedPipeline(object):

    def __init__(self, dbpool):
        self.dbpool = dbpool

    @classmethod
    def from_settings(cls, settings):
        db_params = dict(
            host=settings['MYSQL_HOST'],
            db=settings['MYSQL_DBNAME'],
            user=settings['MYSQL_USER'],
            passwd=settings['MYSQL_PASSWORD'],
            port=settings['MYSQL_PORT'],
            charset='utf8',
            cursorclass=MySQLdb.cursors.DictCursor,
            use_unicode=True
        )

        dbpool = adbapi.ConnectionPool('MySQLdb', **db_params)

        return cls(dbpool)

    def process_item(self, item, spider):
        self.dbpool.runInteraction(self.do_insert, item)
        return item

    def do_insert(self, cursor, item):
        start_time = item['start_time']
        area_name = item['area']
        data = item['data']

        # print("start_time:::" + start_time)
        # print("area_name:::" + area_name)
        # print("data:::")
        # print(data)

        now = datetime.datetime.now()
        create_time = now.strftime('%Y%m%d%H%M%S')
        modify_time = now.strftime('%Y%m%d%H%M%S')

        for weather in reversed(data):

            is_error = 0
            try:
                qx_id = start_time[:10] + area_num_dict[area_name] + weather['od21']
            except Exception as e:
                print(e)
                is_error = 1
                qx_id = uuid.uuid1().get_hex()[16:]

            # print("qx_id:::" + qx_id)
            area = area_dict.get(area_name, '')
            wd = parse_float(weather.get('od22', 0.0))
            sd = parse_float(weather.get('od27', 0.0))
            js = parse_float(weather.get('od26', 0.0))
            fx = weather.get('od24', '暂无风向')
            fl = parse_float(weather.get('od25', 0.0))
            fs = fl_dict.get(fl, 0.0)
            fzy = 0.58 * fs * fs * fs
            fzy = 0.1 if fzy < 0.1 else fzy
            kqzl = parse_float(weather.get('od28', 0.0))

            qx_date = start_time[:10]

            insert_sql = """
                              insert into dm_qx_weather_spider(qx_id, area, wd, sd, js, fx, fl, fs, fzy, kqzl, create_time, 
                              modify_time, qx_date, is_error)
                              values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """
            # print("insert_sql:::" + insert_sql)
            try:
                cursor.execute(insert_sql, (
                    qx_id, area, wd, sd, js, fx, fl, fs, fzy, kqzl, create_time, modify_time, qx_date, is_error))
                # print("插入" + qx_id + "完成！")
            except Exception as e:
                print(e)
                update_sql = """
                                  update dm_qx_weather_spider set wd = %s, sd = %s, js = %s, fl = %s, fs = %s, fzy = %s, kqzl = %s, 
                                  modify_time = %s where qx_id = %s
                                """
                # print("update_sql:::" + update_sql)
                cursor.execute(update_sql, (wd, sd, js, fl, fs, fzy, kqzl, modify_time, qx_id))
                # print("更新" + qx_id + "完成！")
            s_t = datetime.datetime.strptime(start_time, '%Y%m%d%H%M%S')
            start_time = (s_t + datetime.timedelta(hours=1)).strftime('%Y%m%d%H%M%S')
