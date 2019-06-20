# -*- coding: utf-8 -*-
__author__ = 'zhengxin'


def parse_float(string):
    try:
        num = float(string)
    except ValueError as e:
        num = None

    return num


fl_dict = {
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

area_dict = {
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

area_num_dict = {
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
