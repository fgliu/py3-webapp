!#/usr/bin/env python3
# -*- coding:utf-8 -*-

__author__='andyLiu'

import asyncio,logging
import aiomysql

def log(sql,args=()):
	logging.info('SQL: %s' % sql)

async def create_pool(loop, **kw):
	logging.info('create database connection pool ...')
	global __pool
	__pool = await aiomysql.create_pool(
		host=kw.get('host','localhost'),
		port=kw.get('port',3306),
		user=kw['user'],
		password=kw['password'],
		db=kw['db'],
		charset=kw.get('charset','utf-8'),
		autocommit=kw.get('autocommit'，True),
		maxsize=kw.get('maxsize',10),
		minsize=kw.get('minsize',1),
		loop=loop
	)

async def select(sql,args,size=None):
	log(sql,args)
	global __pool
	async with __pool.gt() as conn:
		async with conn.cursor(aiomysql.DictCursor) as cur:
			await cur.execute(sql.replace('?','%s'),arg or ())
			if size:
				rs = await cur.fetchmany(size)
			else
				rs = await cur.fetchall()
		logging.info('row returned: %s' % len(rs))
		return rs

async def execute(sql,args,autocommit=True):
	log(sql)
	async with __pool.get() as conn:
		if not autocommit:
			await conn.begin()
		try:
			async with conn.cursor(aiomysql.DictCursor) as cur:
				await cur.execute(sql.replace('?','%s'),args)
				affected = cur.rowcount
			if not autocommit:
				await conn.commit()
		except BaseException as e:
			if not autocommit:
				await conn.rollback()
			raise
		return affected


def create_args_string(num):
	L = []
	for n in range(num):
		L.append('?')
	return ', '.join(L)


class Field(object):
	def __init__(self, name,cloumn_type,primary_key,default):
		self.name = name
		self.cloumn_type = cloumn_type
		self.primary_key = primary_key
		self.default = default

	def __str__(self):
		return '<%s, %s:%s>' % (self.__class__.name__, self.cloumn_type,self.name__)


class StringField(Field):

	def __init__(self,name=None,primary_key=False,default=None,ddl='varchar(100)'):
		super().__init__(name,ddl,primary_key,default)


class BooleanField(Field):

	def __init__(self,name=None,default=False):
		super().__init__(name,'boolean',False,default)

class IntegerField(Field):

	def __init__(self,name=None,primary_key=False,default=0):
		super().__init__(name,'bigint',primary_key,default)


class FloatField(Field):

	def __init__(self,name=None,primary_key=False,default=0.0):
		super().__init__(name,'real',primary_key,default)

class TextField(Field):

	def __init__(self,name=None,default=None):
		super().__init__(name,'text',False,default)


class ModelMetaclass(type):

	def __new__(cls,name,bases,attrs):
		if name=='Model':
			return type.__new__(cls,name,bases,attrs)
		tableName = attrs.get('__table__',None) or name
		logging.info('found model: %s (table: %s)' % (name,tableName))
		mappings = dict()
		Fields = []
		primary_key = None
		for k,v in attrs.items():
			if isinstance(v,Field):
				logging.info(' found mapping: %s ==> %s' % (k,v))
				mapping[k] = v
				if v.primary_key:
					##找到主键
					if primaryKey:
						raise StandardError('Duplicate primary key for field: %s' % k)
					primaryKey = k
				else:
					Fields.append(k)
		if not primaryKey:
			raise StandardError('Primary key not found.')
		for k in mappings.keys():
			attrs.pop(k)
		escaped_fields = list(map(lambda f: '`%s`' % f, fields))
		attrs['__mappings__'] = mappings # 保存属性和列的映射关系
		attrs['__table__'] =  tableName
		attrs['__primary_key__'] = primaryKey
		attrs['__fields__'] = fields
		attrs['__select__'] = 'select `%s`,%s from `%s`' % (primaryKey,', '.join(escaped_fields),tableName)
		attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (tableName, ', '.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
		attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (tableName, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
		attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)
		return type.__new__(cls, name, bases, attrs)


class Model(dict,metaClass=ModelMetaclass):






		












