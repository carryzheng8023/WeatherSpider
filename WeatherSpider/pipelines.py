# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import MySQLdb
import datetime
import uuid
from WeatherSpider.utils.common import parse_float


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

        self.fl_dict = {
            '0': 0.1,
            '1': 0.9,
            '2': 2.5,
            '3': 4.4,
            '4': 6.7,
            '5': 9.4,
            '6': 12.3,
            '7': 15.5,
            '8': 19,
            '9': 22.6,
            '10': 26.5,
            '11': 30.6,
            '12': 34.8
        }

        self.area_dict = {
            '长沙': '长沙市',
            '湘潭': '湘潭市',
            '株洲': '株洲市',
            '衡阳': '衡阳市',
            '郴州': '郴州市',
            '常德': '常德市',
            '赫山区': '益阳市',
            '娄底': '娄底市',
            '邵阳': '邵阳市',
            '岳阳': '岳阳市',
            '张家界': '张家界市',
            '怀化': '怀化市',
            '永州': '永州市',
            '吉首': '湘西土家族苗族自治州'
        }

        self.area_num_dict = {
            '长沙': '01',
            '湘潭': '02',
            '株洲': '03',
            '衡阳': '04',
            '郴州': '05',
            '常德': '06',
            '赫山区': '07',
            '娄底': '08',
            '邵阳': '09',
            '岳阳': '10',
            '张家界': '11',
            '怀化': '12',
            '永州': '14',
            '吉首': '15'
        }

    def process_item(self, item, spider):

        start_time = item['start_time']
        area_name = item['area']
        data = item['data']

        print("start_time:::" + start_time)
        print("area_name:::" + area_name)
        print("data:::")
        print(data)

        now = datetime.datetime.now()
        create_time = now.strftime('%Y%m%d%H%M%S')
        modify_time = now.strftime('%Y%m%d%H%M%S')

        for weather in reversed(data):

            is_error = 0
            try:
                qx_id = start_time[:10] + self.area_num_dict[area_name] + weather['od21']
            except Exception as e:
                print(e)
                is_error = 1
                qx_id = uuid.uuid1().get_hex()[16:]

            print("qx_id:::" + qx_id)
            area = self.area_dict.get(area_name, '')
            wd = parse_float(weather.get('od22', 0.0))
            sd = parse_float(weather.get('od27', 0.0))
            js = parse_float(weather.get('od26', 0.0))
            fx = weather.get('od24', '暂无风向')
            fl = parse_float(weather.get('od25', 0.0))
            fs = self.fl_dict.get(fl, 0.0)
            fzy = 0.58 * fs * fs * fs
            fzy = 0.1 if fzy < 0.1 else fzy
            kqzl = parse_float(weather.get('od28', 0.0))

            qx_date = start_time[:10]

            insert_sql = """
                      insert into weather_spider(qx_id, area, wd, sd, js, fx, fl, fs, fzy, kqzl, create_time, 
                      modify_time, qx_date, is_error)
                      values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
            # print("insert_sql:::" + insert_sql)
            try:
                self.cursor.execute(insert_sql, (
                    qx_id, area, wd, sd, js, fx, fl, fs, fzy, kqzl, create_time, modify_time, qx_date, is_error))
                self.conn.commit()
                print("插入" + qx_id + "完成！")
            except Exception as e:
                print(e)
                update_sql = """
                          update weather_spider set wd = %s, sd = %s, js = %s, fl = %s, fs = %s, fzy = %s, kqzl = %s, 
                          modify_time = %s where qx_id = %s
                        """
                # print("update_sql:::" + update_sql)
                self.cursor.execute(update_sql, (wd, sd, js, fl, fs, fzy, kqzl, modify_time, qx_id))
                self.conn.commit()
                print("更新" + qx_id + "完成！")
            s_t = datetime.datetime.strptime(start_time, '%Y%m%d%H%M%S')
            start_time = (s_t + datetime.timedelta(hours=1)).strftime('%Y%m%d%H%M%S')

        return item
