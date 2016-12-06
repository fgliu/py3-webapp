#!/usr/bin/env python3
# -*- coding:utf-8 -*-

import orm
from models import User, Blog, Comment
import asyncio
import sys


@asyncio.coroutine
def test(loop):
    yield from orm.create_pool(loop = loop , user='www-data', password='www-data', db='awesome',charset='utf-8')
    u = User(name='Test', email='test@example.com', passwd='1234567890', image='about:blank')
    yield from u.save()
    yield from destory_pool() #这里先销毁连接池

loop = asyncio.get_event_loop()
loop.run_until_complete(test(loop))
loop.close() #然后从容地关闭event loop