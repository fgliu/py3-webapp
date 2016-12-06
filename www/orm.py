#!/usr/bin/env python3
# -*- coding:utf-8 -*-

__author__='andyLiu'

import asyncio,logging
# aiomysql是Mysql的python异步驱动程序，操作数据库要用到
import aiomysql

# 这个函数的作用是输出信息，让你知道这个时间点程序在做什么
def log(sql, args=()):
	logging.info('SQL: %s' % sql)

# 创建全局连接池
# 这个函数将来会在app.py的init函数中引用
# 目的是为了让每个HTTP请求都能s从连接池中直接获取数据库连接
# 避免了频繁关闭和打开数据库连接
async def create_pool(loop, **kw):
	logging.info('create database connection pool ...')
	# 声明变量__pool是一个全局变量，如果不加声明，__pool就会被默认为一个私有变量，不能被其他函数引用
	global __pool
	# 调用一个自协程来创建全局连接池，create_pool的返回值是一个pool实例对象
	__pool = await aiomysql.create_pool(
		# 下面就是创建数据库连接需要用到的一些参数，从**kw（关键字参数）中取出来
        # kw.get的作用应该是，当没有传入参数是，默认参数就是get函数的第二项
		host=kw.get('host','localhost'),
		port=kw.get('port',3306),
		user=kw['user'],
		password=kw['password'],
		db=kw['db'],
		charset=kw.get('charset','utf-8'),
		autocommit=kw.get('autocommit',True),
		maxsize=kw.get('maxsize',10),
		minsize=kw.get('minsize',1),
		loop=loop # 传递消息循环对象，用于异步执行
	)


# =================================以下是SQL函数处理区====================================
# select和execute方法是实现其他Model类中SQL语句都经常要用的方法

# 将执行SQL的代码封装仅select函数，调用的时候只要传入sql，和sql所需要的一些参数就好
# sql参数即为sql语句，args表示要搜索的参数
# size用于指定最大的查询数量，不指定将返回所有查询结果

async def select(sql, args, size=None):
	log(sql, args)
	# 声明全局变量，这样才能引用create_pool函数创建的__pool变量
	global __pool
	# 从连接池中获得一个数据库连接
    # 用with语句可以封装清理（关闭conn)和处理异常工作
	async with __pool.get() as conn:
		# 等待连接对象返回DictCursor可以通过dict的方式获取数据库对象，需要通过游标对象执行SQL
		async with conn.cursor(aiomysql.DictCursor) as cur:
			# 设置执行语句，其中sql语句的占位符为？，而python为%s, 这里要做一下替换
            # args是sql语句的参数
			await cur.execute(sql.replace('?','%s'), args or ())
			# 如果制定了查询数量，则查询制定数量的结果，如果不指定则查询所有结果
			if size:
				rs = await cur.fetchmany(size)
			else:
				rs = await cur.fetchall()
		logging.info('row returned: %s' % len(rs))
		return rs    # 返回结果集


# 定义execute()函数执行insert update delete语句
async def execute(sql, args, autocommit=True):
	# execute()函数只返回结果数，不返回结果集，适用于insert, update这些语句
	log(sql)
	async with __pool.get() as conn:
		if not autocommit:
			await conn.begin()
		try:
			async with conn.cursor(aiomysql.DictCursor) as cur:
				await cur.execute(sql.replace('?','%s'), args)
				affected = cur.rowcount
			if not autocommit:
				await conn.commit()
		except BaseException as e:
			if not autocommit:
				await conn.rollback()
			raise
		return affected


# 这个函数在元类中被引用，作用是创建一定数量的占位符
def create_args_string(num):
	L = []
	for n in range(num):
		L.append('?')
	#比如说num=3，那L就是['?','?','?']，通过下面这句代码返回一个字符串'?,?,?'
	return ', '.join(L)


# =====================================Field定义域区==============================================
# 首先来定义Field类，它负责保存数据库表的字段名和字段类型

# 父定义域，可以被其他定义域继承
class Field(object):
	# 定义域的初始化，包括属性（列）名，属性（列）的类型，主键，默认值
	def __init__(self, name,column_type,primary_key,default):
		self.name = name
		self.column_type = column_type
		self.primary_key = primary_key
		self.default = default    # 如果存在默认值，在getOrDefault()中会被用到

	# 定制输出信息为 类名，列的类型，列名
	def __str__(self):
		return '<%s, %s:%s>' % (self.__class__.__name__, self.column_type,self.name)


class StringField(Field):
	#ddl是数据定义语言("data definition languages")，默认值是'varchar(100)'，意思是可变字符串，长度为100
    #和char相对应，char是固定长度，字符串长度不够会自动补齐，varchar则是多长就是多长，但最长不能超过规定长度

	def __init__(self,name=None,primary_key=False,default=None,ddl='varchar(100)'):
		super().__init__(name,ddl,primary_key,default)


class BooleanField(Field):

	def __init__(self,name=None, default=False):
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


# =====================================Model基类区==========================================


# 编写元类
class ModelMetaclass(type):

	def __new__(cls,name,bases,attrs):
		# 排除Model类本身
		if name=='Model':
			return type.__new__(cls,name,bases,attrs)
		# 获取table名称
		tableName = attrs.get('__table__', None) or name
		logging.info('found model: %s (table: %s)' % (name,tableName))
		# 获取所有定义域中的属性和主键
		mappings = dict()
		fields = []
		primaryKey = None
		for k, v in attrs.items():
			if isinstance(v, Field):
				logging.info(' found mapping: %s ==> %s' % (k, v))
				mappings[k] = v
				if v.primary_key:
					##找到主键
					if primaryKey:	# 若主键已存在,又找到一个主键,将报错,每张表有且仅有一个主键
						raise StandardError('Duplicate primary key for field: %s' % k)
					primaryKey = k
				else:
					fields.append(k)
		# 如果没有找到主键，也会报错
		if not primaryKey:
			raise StandardError('Primary key not found.')
		# 定义域中的key值已经添加到fields里了，就要在attrs中删除，避免重名导致运行时错误
		for k in mappings.keys():
			attrs.pop(k)
		# 将非主键的属性变形,放入escaped_fields中,方便sql语句的书写
		escaped_fields = list(map(lambda f: '`%s`' % f, fields))
		attrs['__mappings__'] = mappings # 保存属性和列的映射关系
		attrs['__table__'] =  tableName
		attrs['__primary_key__'] = primaryKey
		attrs['__fields__'] = fields
		# 构造默认的SELECT, INSERT, UPDATE, DELETE语句
        # 以下都是sql语句
		attrs['__select__'] = 'select `%s`,%s from `%s`' % (primaryKey,', '.join(escaped_fields),tableName)
		attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (tableName, ', '.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
		attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (tableName, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
		attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)
		return type.__new__(cls, name, bases, attrs)


# =====================================Model基类区==========================================


# 定义所有ORM映射的基类Model， 使他既可以像字典那样通过[]访问key值，也可以通过.访问key值
# 继承dict是为了使用方便，例如对象实例user['id']即可轻松通过UserModel去数据库获取到id
# 元类自然是为了封装我们之前写的具体的SQL处理函数，从数据库获取数据
# ORM映射基类,通过ModelMetaclass元类来构造类
class Model(dict, metaclass=ModelMetaclass):

	def __init__(self,**kw):
		super(Model,self).__init__(**kw)

	def __getattr__(self,key):
		try:
			return self[key]
		except KeyError:
			raise AttributeError(r"'Model' object has no attribute '%s'" %key)

	def __setattr__(self,key,value):
		self[key] = value

	def getValue(self,key):
		return __getattr__(self,key,None)

	def getValueOrDefault(self,key):
		value = getattr(self,key,None)
		if value is None:
			field = self.__mappings__[key]
			if field.default is not None:
				value = field.default() if callable(field.default) else field.default
				logging.debug('using default value for %s: %s' % (key,str(value)))
				setattr(self,key,value)
		return value

	@classmethod
	async def findAll(cls,where=None,args=None,**kw):
	    ' find objects by where clause. '
	    sql = [cls.__select__]
	    if where:
	    	sql.append('where')
	    	sql.append(where)
	    if args is None:
	    	args = []
	    orderBy = kw.get('orderBy',None)
	    if orderBy:
	    	sql.append('order by')
	    	sql.append(orderBy)
	    limit = kw.get('limit',None)
	    if limit is not None:
	    	sql.append('limit')
	    	if isinstance(limit,int):
	    		sql.append('?')
	    		args.append(limit)
	    	elif isinstance(limit,tuple) and len(limit) == 2:
	    		sql.append('?, ?')
	    		args.extend(limit)
	    	else:
	    		raise ValueError('Invalid limit value: %s' % str(limit))
	    rs = await select(' '.join(sql),args)
	    return [cls(**r) for r in rs]

	@classmethod
	async def findNumber(cls,selectField,where=None,args=None):
		' find number by select and where. '
		sql = ['select %s _num_ from `%s`' %(selectField,cls.__table__)]
		if where:
			sql.append('where')
			sql.append(where)
		rs = await select(' '.join(sql),args,1)
		if len(rs) == 0:
			return None
		return rs[0]['_num_']

	@classmethod
	async def find(cls, pk):
		' find object by primary key. '
		rs = await select('%s where `%s`=?' %(cls.__select__, cls.__primary_key__), [pk], 1)
		if len(rs) == 0:
			return None
		return cls(**rs[0])

	async def save(self):
		args = list(map(self.getValueOrDefault, self.__fields__))
		args.append(self.getValueOrDefault(self.__primary_key__))
		rows = await execute(self.__update__,args)
		if rows != 1:
			logging.warn('failed to insert by primary key: affected rows: %s ' %rows)


	async def update(self):
		args = list(map(self.getValue, self.__fields__))
		args.append(self.getValue(self.__primary_key__))
		rows = await execute(self.__update__, args)
		if rows != 1:
			logging.warn('failed to update by primary key : affected rows: %s' % rows)

	async def remove(self):
		args = [self.getValue(self.__primary_key__)]
		rows = await execute(self.__delete__, args)
		if rows != 1:
			logging.warn('faild to remove by primary key: affected rows %s' % rows)

@asyncio.coroutine
def destory_pool(): #销毁连接池
    global __pool
    if __pool is not None:
        __pool.close()
        yield from  __pool.wait_closed()



		












