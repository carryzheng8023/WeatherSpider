# -*- coding: utf-8 -*-
__author__ = 'zhengxin'


def parse_float(string):
    try:
        num = float(string)
    except ValueError as e:
        num = None

    return num
