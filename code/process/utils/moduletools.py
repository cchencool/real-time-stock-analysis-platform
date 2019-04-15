#!/usr/bin/env python
# -*- coding: utf-8 -*-

# @Time    : 2019-04-13 13:48
# @Author  : chen
# @Site    : 
# @File    : moduletools.py
# @Software: PyCharm

__author__ = "chen"


def reflect_inst(clz_path, **kwargs):
    package_name = clz_path.rsplit('.', 1)[0]
    clz_name = clz_path.rsplit('.', 1)[1]
    clz_py = __import__(name=package_name, fromlist=[clz_name])
    clz = getattr(clz_py, clz_name)
    inst = clz(**kwargs)
    return inst


def castparam(cast_dict:dict=dict()):
    """
    a decorator to cast input params' type
    :param cast_dict:
    :return:
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            print(f"args: {args}")
            print(f"kwargs: {kwargs}")
            for k in cast_dict:
                if k in kwargs:
                    kwargs.update({k :cast_dict[k](kwargs[k])})
            return func(*args, **kwargs)
        return wrapper
    return decorator



if __name__ == '__main__':
    @castparam({'pid':int, 's':str})
    def test(pid:int, s:str):
        print(f"pid type: {type(pid)}")
        print(f"s type: {type(s)}")

    test(pid="655", s=321)