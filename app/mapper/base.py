import psycopg2
from datetime import datetime, date, timedelta
import random
import json
from main.config import sql_cached

class BulkVar(object):
    def __init__(self, sql_arg, column=None, replace=None, sql_filter={}):
        self.sql_arg = sql_arg
        self.column = column
        self.replace = replace
        self.user_sql_filter = sql_filter

    def sql_replace(self):
        replace = self.replace
        if not replace:
            replace = '%s'
        return replace, (self.sql_arg,)

    def sql_filter(self):
        if self.user_sql_filter:
            return self.user_sql_filter
        if not self.column:
            return {}
        return {self.column: self.sql_arg}


class BulkList(BulkVar):
    def __init__(self, *sql_args, **kwargs):
        BulkVar.__init__(self, sql_args, **kwargs)

    def sql_replace(self):
        replace_sql = self.replace
        if not replace_sql:
            replace_sql = ['%s' for _ in self.sql_arg]
            replace_sql = '(' + ', '.join(replace_sql) + ')'
        return replace_sql, self.sql_arg

    def sql_filter(self):
        if self.user_sql_filter:
            return self.user_sql_filter
        if not self.column:
            return {}
        return {self.column: self.sql_arg[0]}


class DtoAttributeError(Exception):
    pass


class DtoValueError(Exception):
    pass


class Null(object):
    def __nonzero__(self):
        return False


class DtoError(Exception):
    def __init__(self, *args):
        Exception.__init__(self, *args)
        self.attribute = None
        self.type_error = None
        self.error_info = None

    def __repr__(self):
        return '< {0}, {1} >'.format(self.__class__.__name__, self.__dict__)


class DtoType(object):
    pass


class DtoInteger(DtoType):
    def __repr__(self):
        return 'integer'


class DtoNumeric(DtoType):
    def __repr__(self):
        return 'numeric'


class DtoText(DtoType):
    def __repr__(self):
        return 'text'


class DtoBytea(DtoType):
    def __repr__(self):
        return 'binary'


class DtoBoolean(DtoType):
    def __repr__(self):
        return 'boolean'


class DtoTimestamp(DtoType):
    def __repr__(self):
        return 'timestamp'


class DtoDate(DtoType):
    def __repr__(self):
        return 'date'


class DtoTimeDelta(DtoType):
    def __repr__(self):
        return 'time interval'


class DtoUom(DtoType):
    def __init__(self, base_uom):
        self.base_uom = base_uom

    def __repr__(self):
        return 'Uom'


class DtoObject(DtoType):
    def __repr__(self):
        return 'object'


class Dto(object):
    def __init__(self, **kwargs):
        self._attr_type = {}
        self._nullable = {}
        self._has_default = {}
        self._default_value = {}
        self._read_only = {}
        self.attributes = []

    def __iter__(self):
        for attr in self.attributes:
            if attr not in self.__dict__:
                continue
            yield attr, self.__dict__[attr]

    def iter(self, include_read_only=True, include_none=False):
        for attr, value in self:
            if not include_read_only and self._read_only[attr]:
                continue
            if not include_none and value is None:
                continue
            yield attr, value

    def __nonzero__(self):
        for attr, value in self:
            if value != self._default_value[attr]:
                return True
        return False

    def get_attr(self, attr_name):
        return self.__dict__[attr_name]

    def add_attr(self, attr_name, attr_type=None, nullable=True, has_default=False, value=None, read_only=False):
        if attr_name in self.attributes:
            return
        self.attributes.append(attr_name)

        self.__dict__[attr_name] = value
        self._default_value[attr_name] = value
        self._attr_type[attr_name] = attr_type
        self._nullable[attr_name] = nullable
        self._has_default[attr_name] = has_default
        self._read_only[attr_name] = read_only

    def rename_attr(self, old_attr, new_attr):
        self.attributes.append(new_attr)
        self.__dict__[new_attr] = self.__dict__[old_attr]
        self._attr_type[new_attr] = self._attr_type[old_attr]
        self._nullable[new_attr] = self._nullable[old_attr]
        self._has_default[new_attr] = self._has_default[old_attr]
        self._read_only[new_attr] = self._read_only[old_attr]
        self.del_attr(old_attr)

    def del_attr(self, attr_name):
        del self.__dict__[attr_name]
        del self._attr_type[attr_name]
        del self._nullable[attr_name]
        del self._has_default[attr_name]
        del self._read_only[attr_name]
        self.attributes.remove(attr_name)

    def properties(self):
        prop = {}
        for attr in self.attributes:
            prop[attr] = {}
            try:
                prop[attr]['type'] = self._attr_type[attr]
            except:
                prop[attr]['type'] = None
            try:
                if isinstance(self._attr_type[attr], DtoUom):
                    prop[attr]['unit'] = self._attr_type[attr].base_uom
            except:
                pass
            try:
                prop[attr]['nullable'] = self._nullable[attr]
            except:
                prop[attr]['nullable'] = None
            try:
                prop[attr]['has_default'] = self._has_default[attr]
            except:
                prop[attr]['has_default'] = None
            try:
                prop[attr]['read_only'] = self._read_only[attr]
            except:
                prop[attr]['read_only'] = None
        return prop

    def convert_type(self, attr, value):
        try:
            if value is None or isinstance(value, Null):
                return value

            if isinstance(self._attr_type[attr], DtoInteger):
                return int(value)
            if isinstance(self._attr_type[attr], DtoNumeric):
                return float(value)
            if isinstance(self._attr_type[attr], DtoText):
                if not isinstance(value, str):
                    raise ValueError()
            if isinstance(self._attr_type[attr], DtoBytea):
                return psycopg2.Binary(value)
            if isinstance(self._attr_type[attr], DtoBoolean):
                return bool(value)
            if isinstance(self._attr_type[attr], DtoTimestamp):
                if not isinstance(value, (datetime, date)):
                    raise ValueError()
            if isinstance(self._attr_type[attr], DtoDate):
                if not isinstance(value, date):
                    raise ValueError()
            if isinstance(self._attr_type[attr], DtoTimeDelta):
                if not isinstance(value, timedelta):
                    raise ValueError()
            return value
        except Exception as e:
            dto_error = DtoError(e)
            dto_error.type_error = 'ValueError'
            dto_error.attribute = attr
            raise dto_error

    def check_null(self, attr, value):
        if isinstance(value, Null) and not self._nullable[attr]:
            dto_error = DtoError()
            dto_error.type_error = 'NullError'
            dto_error.attribute = attr
            raise dto_error

    def check_attr(self, attr, value):
        if attr not in self.attributes:
            # print attr, value
            dto_error = DtoError()
            dto_error.type_error = 'AttributeError'
            dto_error.attribute = attr
            raise dto_error

    def check_default(self, attr):
        value = self.__dict__.get(attr)
        if (value is None or isinstance(value, Null)) and not self._nullable[attr] and not self._has_default[attr]:
            # print attr, value
            dto_error = DtoError()
            dto_error.type_error = 'NullError'
            dto_error.attribute = attr
            raise dto_error

    def update_attr(self, attr, value):
        self.check_attr(attr, value)
        self.__dict__[attr] = self.convert_type(attr, value)

    def update(self, **values):
        for attr in values:
            self.update_attr(attr, values[attr])

    def update_values(self, **values):
        for attr in self.attributes:
            self.__dict__[attr] = values.get(attr)

    def update_attr_no_check(self, attr, value):
        self.__dict__[attr] = value

    def update_no_check(self, **values):
        for attr in values:
            self.update_attr_no_check(attr, values[attr])

    def clear(self):
        for key in self.attributes:
            self.__dict__[key] = None

    def __repr__(self):
        return '< {0}, {1} >'.format(self.__class__.__name__, dict(self))

    def __str__(self):
        ret = dict()
        for att, value in self.iter():
            ret[att] = value
        try:
            return json.dumps(ret, indent=4, sort_keys=True)
        except:
            return str(ret)

class SqlBuilder(object):
    def __init__(self):
        self.insert = {}
        self.select = {}
        self.update = {}
        self.where = {}

    def add_insert(self, column, value, arg=[]):
        self.insert[column] = {}
        self.insert[column]['value'] = value
        self.insert[column]['arg'] = list(arg)

    def add_select(self, column, value=None, arg=[]):
        self.select[column] = {}
        self.select[column]['value'] = value
        self.select[column]['arg'] = list(arg)

    def add_update(self, column, operator='=', replace='%s', arg=[]):
        self.update[column] = {}
        self.update[column]['replace'] = replace
        self.update[column]['operator'] = operator
        self.update[column]['arg'] = list(arg)

    def add_where(self, column, operator='=', replace='%s', arg=[]):
        self.where[column] = self.where.get(column, {})
        self.where[column][operator] = self.where[column].get(operator, {})
        self.where[column][operator]['replace'] = replace
        self.where[column][operator]['arg'] = list(arg)

    def delete_insert(self, column):
        del self.insert[column]

    def delete_select(self, column):
        del self.select[column]

    def delete_update(self, column):
        del self.update[column]

    def delete_where(self, column):
        del self.where[column]

    def convert_args(self, args, raise_error_on_none=True):
        coverted = []
        for arg in args:
            if arg is None:
                raise ValueError()
            if isinstance(arg, Null):
                arg = None
            coverted.append(arg)
        return coverted

    def get_insert(self):
        column_list = []
        value_list = []
        arg_list = []
        for column in self.insert:
            try:
                arg_list = arg_list + self.convert_args(self.insert[column]['arg'])
            except ValueError:
                # do not include none
                continue
            column_list.append(column)
            value_list.append(self.insert[column]['value'])
        column_list = ', '.join(column_list)
        value_list = ', '.join(value_list)
        return column_list, value_list, tuple(arg_list)

    def get_where(self, sep=' AND ', prefix=''):
        where_list = []
        arg_list = []
        for column in self.where:
            for operator in self.where[column]:
                try:
                    args = self.convert_args(self.where[column][operator]['arg'])
                    arg_list = arg_list + args
                except ValueError:
                    # do not include none
                    continue
                where_list.append('{0}{1} {2} {3}'.format(prefix, column, operator, self.where[column][operator]['replace']))
        where_list = sep.join(where_list)
        if not where_list:
            where_list = True
        return where_list, tuple(arg_list)

    def get_update(self, prefix=''):
        update_list = []
        arg_list = []
        for column in self.update:
            try:
                arg_list = arg_list + self.convert_args(self.update[column]['arg'])
            except ValueError:
                # do not include none
                continue
            update_list.append('{0}{1} {2} {3}'.format(prefix, column, self.update[column]['operator'], self.update[column]['replace']))
        update_list = ', '.join(update_list)
        return update_list, tuple(arg_list)

    def get_select(self, prefix=''):
        select_list = []
        arg_list = []
        for column in self.select:
            arg_list = arg_list + self.convert_args(self.select[column]['arg'], raise_error_on_none=False)
            if self.select[column]['value']:
                select_list.append('{0} AS {1}'.format(self.select[column]['value'], column))
            else:
                select_list.append('{0}{1}'.format(prefix, column))
        select_list = ', '.join(select_list)
        return select_list, tuple(arg_list)


class Mapper(object):
    def __init__(self, db, DtoClass, table_name='', table_name_id_seq=''):
        self.DtoClass = DtoClass
        self.db = db
        self.table_name = table_name
        self.table_name_id_seq = table_name_id_seq

    async def insert(self, dto, dto_id=None):
        dto = await self.convert_to_db(dto)
        sql_builder = self.get_sql_builder(dto)

        if dto_id:
            sql_builder.add_insert("id", '%s', (dto_id,))

        column_list, value_list, insert_arg_list = sql_builder.get_insert()
        select_list, select_arg_list = sql_builder.get_select()

        sql = "INSERT INTO {0} ({1}) VALUES %b ON CONFLICT DO NOTHING RETURNING {2}".format(self.table_name, column_list, select_list)

        if dto_id:
            sql_args = (BulkList(*insert_arg_list, replace='(' + value_list + ')', sql_filter={"id": dto_id}),) + select_arg_list
        else:
            sql_args = (BulkList(*insert_arg_list, replace='(' + value_list + ')', sql_filter={}),) + select_arg_list

        sql_args = self.get_cache_args(sql_args)
        new_args = []
        for args in sql_args:
            new_args += list(args)

        value_str = await self.get_insert_value_str(sql_args)
        sql = sql.replace('%b', value_str)

        db_return = await self.db.fetch_one_row(sql, args=new_args)

        print("insert", db_return)
        if not db_return:
            return
        dto = self.get_dto(**db_return)
        return dto

    async def get_insert_value_str(self, sql_args):
        value_str = ""
        para_count = 1
        for args in sql_args:
            args_str = "("
            vals = []
            for val in args:
                vals.append("${}".format(para_count))
                para_count += 1
            args_str += ",".join(vals)
            args_str += ")"

            value_str += args_str

        return value_str

    async def select(self, dto, where_column):
        dto = await self.convert_to_db(dto)
        sql_builder = self.get_sql_builder(dto)
        select_list, select_arg_list = sql_builder.get_select()
        sql = """SELECT {0} FROM {1} WHERE {2} IN ($1)""".format(select_list, self.table_name, where_column)
        sql_args = select_arg_list + (BulkVar(dto.get_attr(where_column), column=where_column),)
        sql_args = self.get_cache_args(sql_args)
        print(sql_args)
        db_return = sql_cached.get_cache("business {where} = {value}".format(where=where_column,
                                                                             value=sql_args))

        print(sql)
        if not db_return:
            print("no cached")
            db_return = await self.db.fetch_one_row(sql, args=sql_args)
            if not db_return:
                return
            db_return = {k.strip(): v for k, v in zip(select_list.split(','), db_return)}
            if db_return:
                sql_cached.set_cache(cache_id="business {where} = {value}".format(where=where_column,
                                                                                  value=sql_args),
                                     value=db_return)
        dto = self.get_dto(**db_return)
        return dto

    async def update(self, dto, where_column='id'):
        dto = await self.convert_to_db(dto)
        sql_builder = self.get_sql_builder(dto)
        update_list, update_arg_list = sql_builder.get_update()
        select_list, select_arg_list = sql_builder.get_select()

        sql = 'UPDATE {0} SET {1} WHERE {2} = %s RETURNING {3}'.format(self.table_name, update_list, where_column, select_list)
        sql_args = update_arg_list + (dto.get_attr(where_column),) + select_arg_list
        sql_args = self.get_cache_args(sql_args)
        print("update", sql, sql_args)
        db_return = self.static_db.fetch_one_row(sql, args=sql_args, dict_cursor=True)

        dto = self.get_dto(**db_return.query_data)
        return dto

    def get_properties(self):
        dto = self.get_dto().properties()
        return dto

    def get_cache_args(self, args):
        cache_args = []
        for arg in args:
            if isinstance(arg, BulkVar):
                arg = arg.sql_arg
            cache_args.append(arg)
        cache_args = tuple(cache_args)
        return cache_args

    def get_sql_builder(self, dto):
        sql_builder = SqlBuilder()
        for column, value in dto:
            operator = '='
            if isinstance(value, list):
                value = tuple(value)
            if isinstance(value, tuple):
                operator = 'IN'
            sql_builder.add_select(column)
            sql_builder.add_where(column, operator, '%s', (value,))
            if dto._read_only[column]:
                continue
            sql_builder.add_insert(column, '%s', (value,))
            sql_builder.add_update(column, operator, '%s', (value,))
        return sql_builder

    def get_dto(self, **kwargs):
        return self.DtoClass(**kwargs)

    def get_dtod(self, dict_key='id', **kwargs):
        multi_val = kwargs[dict_key]
        del kwargs[dict_key]

        dtod = {}
        for val in multi_val:
            dto = self.get_dto(**kwargs)
            dto.__dict__[dict_key] = val
            dtod[val] = dto
        return dtod

    def get_dtol(self, **kwargs):
        dto = self.get_dto()
        for column in kwargs:
            if isinstance(kwargs[column], list) or isinstance(kwargs[column], tuple):
                continue
            dto.update(**{column: kwargs[column]})

        dtol = []
        for column in kwargs:
            if isinstance(kwargs[column], list) or isinstance(kwargs[column], tuple):
                for value in kwargs[column]:
                    dto_temp = self.get_dto(**dict(dto))
                    dto_temp.update(**{column: value})
                    dtol.append(dto_temp)
        if not dtol:
            dtol = [dto]
        return dtol


    def convert_to_user(self, dto):
        dto = self.get_dto(**dict(dto))
        return dto


    async def convert_to_db(self, dto):
        dto = self.get_dto(**dict(dto))
        for attr, value in dto:
            dto.check_null(attr, value)
        return dto




