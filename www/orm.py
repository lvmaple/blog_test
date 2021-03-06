import logging
import aiomysql
from datetime import datetime


def log(sql, args=()):
    logging.info('SQL: {}'.format(sql))


async def create_pool(loop, **kw):
    logging.info(str(datetime.now()) + ":create database connection pool....")
    global __pool
    __pool = await aiomysql.create_pool(
        host=kw.get('host', 'localhost'),
        port=kw.get('port', 3306),
        user=kw['user'],
        password=kw['password'],
        db=kw['db'],
        charset=kw.get('charset', 'utf8'),
        autocommit=kw.get('autocommit', True),
        maxsize=kw.get('maxsize', 10),
        minsize=kw.get('minsize', 1),
        loop=loop
    )


async def destroy_pool():
    logging.info(str(datetime.now()) + ":destroy database connection pool....")
    global __pool
    if __pool is not None:
        __pool.close()
        await __pool.wait_closed()


async def select(sql, args, size=None):
    log(sql, args)
    global __pool
    # with (await __pool) as conn:
    async with __pool.get() as conn:
        # cur = await conn.cursor(aiomysql.DictCursor)
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql.replace('?', '%s'), args or ())
            if size:
                rs = await cur.fetchmany(size)
            else:
                rs = await  cur.fetchall()
            await cur.close()
        logging.info('rows returned: {}'.format(len(rs)))
        return rs


async def execute(sql, args, autocommit=True):
    log(sql)
    # with (await __pool) as conn:
    async with __pool.get() as conn:
        if not autocommit:
            await conn.begin()
        try:
            # cur = await conn.cursor()
            async with conn.cursor(aiomysql.DictCursor) as cur:
                try:
                    await cur.execute(sql.replace('?', '%s'), args)
                except Exception as e:
                    print(str(e))
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
    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default

    def __str__(self):
        return '<{}, {}:{}>'.format(self.__class__.__name__, self.column_type, self.name)


class StringField(Field):
    def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
        super().__init__(name, ddl, primary_key, default)


class BooleanField(Field):
    def __init__(self, name=None, default=False):
        super().__init__(name, 'boolean', False, default)


class IntegerField(Field):
    def __init__(self, name=None, primary_key=False, default=0):
        super().__init__(name, 'bigint', primary_key, default)


class FloatField(Field):
    def __init__(self, name=None, primary_key=False, default=0.0):
        super().__init__(name, 'real', primary_key, default)


class TextField(Field):
    def __init__(self, name=None, default=None):
        super().__init__(name, 'text', False, default)


class ModelMetaclass(type):

    def __new__(cls, name, bases, attrs):
        # 排除Model类本身
        if name == 'Model':
            return type.__new__(cls, name, bases, attrs)
        # 获取table名
        tableName = attrs.get('__table__', None) or name
        logging.info(str(datetime.now()) + 'found model: {} (table: {})'.format(name, tableName))
        # 获取所有的field和主键名
        mappings = dict()
        #存放除主键外的其它
        fields = []
        primaryKey = None
        for k, v in attrs.items():
            if isinstance(v, Field):
                logging.info(str(datetime.now()) + ' found mapping: {} ==> {}'.format(k, v))
                mappings[k] = v
                if v.primary_key:
                    # 找到主键
                    if primaryKey:
                        raise RuntimeError('Duplicate primary key for field: {}'.format(k))
                    primaryKey = k
                else:
                    fields.append(k)
        if not primaryKey:
            raise RuntimeError('Primary key not found.')
        for k in mappings.keys():
            attrs.pop(k)
        escaped_fields = list(map(lambda f: '`{}`'.format(f), fields))
        attrs['__mappings__'] = mappings  # 保存属性和列的映射关系
        attrs['__table__'] = tableName
        attrs['__primary_key__'] = primaryKey
        attrs['__fields__'] = fields
        # 构造默认的select，insert， update和delete语句
        attrs['__select__'] = 'select `{}`, {} from `{}`'.format(primaryKey, ','.join(escaped_fields), tableName)
        attrs['__insert__'] = 'insert into `{}` ({}, `{}`) values ({})'.format(tableName, ','.join(escaped_fields),
                                                                               primaryKey, create_args_string(
                len(escaped_fields) + 1))
        attrs['__update__'] = 'update `{}` set {} where `{}`=?'.format(tableName, ','.join(map(lambda f: '`{}`=?'.
                                                                                               format(
            mappings.get(f).name or f),
                                                                                               fields)), primaryKey)
        attrs['__delete__'] = 'delete from `{}` where `{}`=?'.format(tableName, primaryKey)
        return type.__new__(cls, name, bases, attrs)


class Model(dict, metaclass=ModelMetaclass):
    def __init__(self, **kw):
        super(Model, self).__init__(**kw)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '{}'".format(key))

    def __setattr__(self, key, value):
        self[key] = value

    def getvalue(self, key):
        return getattr(self, key, None)

    def getvalueordefault(self, key):
        value = getattr(self, key, None)
        if value is None:
            field = self.__mappings__[key]
            if field.default is not None:
                value = field.default() if callable(field.default) else field.default
                logging.debug(str(datetime.now()) + 'using default value for {}: {}'.format(key, str(value)))
                setattr(self, key, value)
        return value

    @classmethod
    async def find(cls, pk):
        ' find object by primary key. '
        rs = await select('{} where `{}`=?'.format(cls.__select__, cls.__primary_key__), [pk], 1)
        if len(rs) == 0:
            return None
        return cls(**rs[0])

    @classmethod
    async def findall(cls, where=None, args=None, **kw):
        '''
            find objects by where clause.
        '''
        sql = [cls.__select__]
        if where:
            sql.append('where')
            sql.append(where)
        if args is None:
            args = []
        orderBy = kw.get('orderBy', None)
        if orderBy:
            sql.append('order by')
            sql.append(orderBy)
        limit = kw.get('limit', None)
        if limit is not None:
            sql.append('limit')
            if isinstance(limit, int):
                sql.append('?')
                args.append(limit)
            elif isinstance(limit, tuple) and len(limit) == 2:
                sql.append('?,?')
                args.extend(limit)
            else:
                raise ValueError('Invalid limit value: {}'.format(str(limit)))
        logging.info(str(datetime.now()) + ' {!s}.'.format(' '.join(sql)))
        rs = await select(' '.join(sql), args)
        return [cls(**r) for r in rs]

    @classmethod
    async def findnumber(cls, selectField, where=None, args=None):
        ' find number by select and where. '
        sql = ['selevt {} _num_ from `{}`'.format(selectField, cls.__table__)]
        if where:
            sql.append('where')
            sql.append(where)
        rs = await select(' '.join(sql), args, 1)
        if len(rs) == 0:
            return None
        return rs[0]['_num_']

    async def save(self):
        args = list(map(self.getvalueordefault, self.__fields__))
        args.append(self.getvalueordefault(self.__primary_key__))
        rows = await execute(self.__insert__, args)
        if rows != 1:
            logging.warning(str(datetime.now()) + ' failed to insert record: affected rows: {}'.format(rows))

    async def update(self):
        args = list(map(self.getvalue, self.__fields__))
        args.append(self.getValue(self.__primary_key__))
        rows = await execute(self.__update__, args)
        if rows != 1:
            logging.warning('failed to update by primary key: affected rows: %s' % rows)

    async def remove(self):
        args = [self.getvalue(self.__primary_key__)]
        rows = await execute(self.__delete__, args)
        if rows != 1:
            logging.warning('failed to remove by primary key: affected rows: %s' % rows)

