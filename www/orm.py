#-coding:utf-8-*-
__author__ = 'dingding copy Liao'

import asyncio,logging

import aiomysql

def log(sql,args=()):
    logging.info('SQL:%s' % sql)

#创建连接池
@asyncio.coroutine
def create_pool(loop,**kwargs):
    logging.info('create database connection pool')
    global __pool
    __pool = yield from aiomysql.create_pool(
        host=kwargs.get('host','localhost'),
        port=kwargs.get('port',3306),
        user=kwargs['user'],
        password=kwargs['password'],
        db=kwargs['db'],
        charset=kwargs.get('charset','utf8'),
        autocommit=kwargs.get('autocommit',True),
        maxsize=kwargs.get('maxsize',10),
        minsize=kwargs.get('minsize',1),
        loop=loop
    )

#select语句
@asyncio.coroutine
def select(sql,args,size=None):
    log(sql,args)
    global __pool
    with (yield from __pool) as conn:
        cur = yield from conn.cursor(aiomysql.DictCursor)
        yield from cur.execute(sql.replace('?','%s'),args or ())
        if size:
            rs = yield from cur.fetchmany(size)
        else:
            rs = yield from cur.fetchall()
        yield from cur.close()
        logging.info('rows returned:%s'%len(rs))
        return rs

#定义execute函数，执行数据库的inset,update,delete
@asyncio.coroutine
def execute(sql,args):
    log(sql)
    with (yield from __pool) as conn:
        try:
            cur = yield from conn.cursor()
            yield from cur.execute(sql.replace('?','%s'),args)
            affected = cur.rowcount
            yield from cur.close()
        except BaseException as e:
            raise
        return affected

#ORM
from orm import Model,StringField,IN




