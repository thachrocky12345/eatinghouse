#! /user/bin/python

# imports
from collections import namedtuple
import os
import time

import psycopg2
from psycopg2 import extras
from pandas.io import sql as psql
import logging

ON_CONFLICT = ' ON CONFLICT DO NOTHING '

#convert dec to float
try:
    DEC2FLOAT = psycopg2.extensions.new_type(
        psycopg2.extensions.DECIMAL.values,
        'DEC2FLOAT',
        lambda value, curs: float(value) if value is not None else None)
except AttributeError:
    DEC2FLOAT = psycopg2.extensions.new_type(
        psycopg2._psycopg.DECIMAL.values,
        'DEC2FLOAT',
        lambda value, curs: float(value) if value is not None else None)

psycopg2.extensions.register_type(DEC2FLOAT)

# Execution types
FETCH_ONE = 'fetchone'
FETCH_ALL = 'fetchall'
MODIFY = 'modify'

ExecutionResults = namedtuple('ExecutionResults', ['query_data', 'row_count', 'cursor_description'])


class NullHandler(logging.Handler):
    def emit(self, record):
        pass

log = logging.getLogger('DataConnector')
log.addHandler(NullHandler())

RECONNECT_ATTEMPTS = 10




class Block(object):
    _sql = None
    _values = None

    def __init__(self, db, header, template, data, ret=False):
        """

        :param db:
        :param header: is '''insert into table(col1,col2)'''
        :param template: is '''({} , {})''' or '''({col1} , {col2})'''
        :param data: list of list or list of dict
        """
        self.db = db
        self.header = header
        self.template = template
        self.data = data

        self.return_values = None
        self.ret = ret

    @property
    def values(self):
        self._values = []

        if self.is_dict() is True:
            for instance in self.data:
                self._values.append(self.template.format(**instance))
        else:
            for instance in self.data:
                self._values.append(self.template.format(*instance))

        self._values = ",".join(self._values)

        self._values = '''{}'''.format(self._values) + ON_CONFLICT
        if self.ret is True:
            return self._values + " RETURNING id "
        return self._values

    @property
    def sql(self):

        return self.header + ' values ' + self.values

    @property
    def set_statement(self):
        raise NotImplementedError

    def execute(self):
        # print (self.sql)
        if self.ret is True:
            self.return_values = self.db.fetch_all_rows(self.sql).query_data
        else:
            self.db.modify_rows(self.sql)

        return self.return_values

    def is_dict(self):
        if isinstance(self.data[0], dict):
            is_dict = True
        elif isinstance(self.data[0], list):
            is_dict = False
        else:
            raise ValueError("Either dict or list only")
        return is_dict


class BlockUpdate(Block):
    def __init__(self, db, header, template, data, keys, update_cols=None, ret=False):

        Block.__init__(self, db, header, template, data, ret=ret)

        self.header = self.header + ' update_tb '
        self.keys = keys
        self.update_cols = update_cols
        self.ret = ret

        assert isinstance(keys, list), 'Must provide list of keys for update'

        self.check_keys(keys)

        assert isinstance(self.data[0], dict), 'Data must be a dictionary'

    def check_keys(self, keys):
        for col in keys:
            if col not in self.data[0]:
                raise ValueError("{} is Invalid key".format(col))

    @property
    def set_statement(self):

        if self.update_cols is None:
            self.update_cols = [col for col in self.data[0] if col not in self.keys]

        ret_set_stm = ''' SET {}  FROM '''

        modified_updates = []
        for col in self.update_cols:
            modified_updates.append('{col}=data.{col}'.format(col=col))

        modified_updates = ','.join(modified_updates)
        return ret_set_stm.format(modified_updates)

    @property
    def data_statement(self):
        ret_data_statement = ''' AS data({}) WHERE '''
        ordered_cols = []
        ordered_template = self.template.split(',')[::-1]
        while len(ordered_template) > 0:
            model = ordered_template.pop()
            for col in self.data[0].keys():
                if col in model:
                    ordered_cols.append(col)

        ordered_cols = ','.join(ordered_cols)
        return ret_data_statement.format(ordered_cols)

    @property
    def conditional_statement(self):

        conditional_cols = []
        for key in self.keys:
            conditional_cols.append('update_tb.{key}=data.{key}'.format(key=key))
        if self.ret is False:
            return ' AND '.join(conditional_cols)

        return_statement = ','.join(['update_tb.{}'.format(col) for col in self.data[0].keys()])
        return ' AND '.join(conditional_cols) + ' RETURNING ' + return_statement

    @property
    def values(self):
        ret_values = ''' (VALUES {}) '''
        values = []

        if self.is_dict() is True:
            for instance in self.data:
                values.append(self.template.format(**instance))
        else:
            for instance in self.data:
                values.append(self.template.format(*instance))

        values = ",".join(values)

        values = '''{}'''.format(values)
        return ret_values.format(values)

    @property
    def sql(self):
        return self.header + self.set_statement +\
               self.values + self.data_statement + \
               self.conditional_statement


class BlockList(Block):
    def __init__(self, db, header, template, data, ret=False):
        Block.__init__(self, db, header, template, data, ret=ret)

    @property
    def values(self):
        temp_values = [self.template] * len(self.data)
        return ",".join(temp_values)

    @property
    def args(self):
        ret = []
        for row in self.data:
            ret += row
        return ret

    def execute(self):
        self.db.execute_row(self.sql, *self.args)




class BulkDb(object):

    _divider = None

    def __init__(self, db):
        self.db = db


    def update(self, header, template, data, keys, update_cols=None, ret=False, block=3000):
        """
        :param data: data must be list or iter
        header = '''Update test'''
        template = ''' ({id}, {col1}, {col2})'''
        bulk_insert = BulkDb(db=db3)
        ret_data = bulk_insert.update(header=header, template=template,
                    data=update_list, keys=['id'],
                    update_cols= ['col2'],block=10, ret=True)
        """
        assert isinstance(data, list), "Data must be a list"

        divider = len(data) // block
        start = 0
        end = 0

        return_values = None

        if ret is True:
            return_values = []

        for index in range(0, divider + 1):
            end = start + block
            if end >= len(data):
                end = len(data)
            if start == end:
                continue

            exec_block = BlockUpdate(db=self.db, header=header, template=template, data=data[start:end],
                                     keys=keys, update_cols=update_cols, ret=ret)
            if ret is True:
                return_values += exec_block.execute()
            else:
                exec_block.execute()
            start = end

        return return_values

    def insert_list(self, header, template, data, block=3000):
        """

        :param data: data must be list of iter

        with list
        header = '''Insert into test(col1, col2)'''
        template = ''' (%s, %s)'''
        bulk_insert = BulkDb(db=db3 )
        bulk_insert.insert(header=header, template=template, data=list_data, block=10)
        """
        assert isinstance(data, list), "Data must be a list"

        divider = len(data) // block
        start = 0
        end = 0
        return_values = None
        for index in range(0, divider + 1):
            end = start + block
            if end >= len(data):
                end = len(data)
            if start == end:
                continue
            exec_block = BlockList(db=self.db, header=header, template=template, data=data[start:end])
            exec_block.execute()
            start = end
        return return_values



    def insert(self, header, template, data, block=3000, ret=False):
        """
        :param data: data must be list of iter

        with list
        header = '''Insert into test(col1, col2)'''
        template = ''' ({}, {})'''
        bulk_insert = BulkDb(db=db3 )
        bulk_insert.insert(header=header, template=template, data=list_data, block=10)

        with dict
        header = '''Insert into test(col1, col2)'''
        template = ''' ({col1}, {col2})'''
        bulk_insert = BulkDb(db=db3)
        ret_data = bulk_insert.insert(header=header, template=template, data=list_data, block=10, ret=True)
        """
        assert isinstance(data, list), "Data must be a list"

        divider = len(data) // block
        start = 0
        end = 0
        return_values = None
        if ret is True:
            return_values = []

        for index in range(0, divider + 1):

            end = start + block
            if end >= len(data):
                end = len(data)
            if start == end:
                continue
            exec_block = Block(db=self.db, header=header, template=template, data=data[start:end], ret=ret)
            if ret is True:
                return_values += exec_block.execute()
            else:
                exec_block.execute()
            start = end
        return return_values

class PgsqlExecutor(object):
    """
    config is dictionary saving database info such as: host, port, user, password, database
    """
    def __init__(self, config, auto_commit=True):

        if isinstance(config, dict):
            self.config = config
        else:
            raise ValueError("config must be a dict instance")

        self.cursor = None
        self.connection = None
        self.auto_commit = auto_commit
        self.__connect__()

    def __connect__(self):
        for retry in range(RECONNECT_ATTEMPTS, -1, -1):
            try:
                self._reconnect()
                self.connection.autocommit = self.auto_commit
                break
            except psycopg2.OperationalError:
                if retry == 0:
                    raise
                log.warning('Could not connect to Database. Will retry in 10 seconds. {} retries left'.format(retry))
                time.sleep(10)

    def _reconnect(self):
        self.connection = psycopg2.connect("dbname={database}"
                                           " user={user}"
                                           " password={password}"
                                           " host={host}"
                                           " port={port}".format(**self.config))
        self.cursor = self.connection.cursor()

    def get_child_tables(self, table_name):
        """
        get all child table
        :param table_name: should be include schema, else it assume to be 'public'
        :return: results = ExecutionResults
        """
        assert isinstance(table_name, str), "table_name must be a string"

        sql = '''
        SELECT c.relname FROM pg_catalog.pg_class c
            LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = '{schema}'
            AND c.relname like '{table}%' and c.relname != '{table}' order by 1
        '''
        if '.' in table_name:
            schema, table = table_name.split('.')
        else:
            schema = 'public'
            table = table_name

        return self.fetch_all_rows(sql.format(schema=schema, table=table)).query_data

    def get_dataframe(self, sql, args=None):
        """
        This will be used in machine learning to generate dataframe data for statistical report
        :param sql:
        :param args:
        :return:
        """
        log.debug("""executing cursor to dataframe""")
        if args:
            log.debug("""sql to be executed: {}""".format(sql%(args)))
        else:
            log.debug("""sql to be executed: {}""".format(sql))

        return psql.read_sql(sql, con=self.connection, params=args)

    def execute_row(self, sql, *args, **kwargs):
        if kwargs:
            args = kwargs
        else:
            args = args

        self.cursor.execute(sql, args)

    def _execute(self, sql, args, execution_type, dict_cursor=False):
        """
        Execute a select statement and fetch a single row.
        query_data return nametupled

        reconnect if connect is closed() or self.connection.closed != 0
        """

        if self.connection.closed != 0:
            self.__connect__()

        if dict_cursor is False:
            cursor_type = extras.NamedTupleCursor
        else:
            cursor_type = extras.RealDictCursor

        with self.connection.cursor(cursor_factory=cursor_type) as cursor:
            log.debug(cursor.mogrify(sql, args))
            print(cursor.mogrify(sql, args))
            cursor.execute(sql, args)
            if execution_type == FETCH_ONE:
                query_data = cursor.fetchone()
            elif execution_type == FETCH_ALL:
                query_data = cursor.fetchall()
            else:
                query_data = None

            results = ExecutionResults(
                query_data=query_data,
                row_count=cursor.rowcount,
                cursor_description=cursor.description if execution_type == MODIFY else None
            )

        return results

    @staticmethod
    def convert_result_to_namedtuple(cursor_description, query_data):
        namedtuple_result = namedtuple('Result', [col.name for col in cursor_description])
        if query_data is None:
            converted_result = None
        elif isinstance(query_data, list):
            converted_result = [namedtuple_result(*row) for row in query_data]
        else:
            converted_result = namedtuple_result(*query_data)

        return converted_result

    def fetch_one_row(self, sql, args=None, dict_cursor=False):
        """
        Execute a select statement and fetch a single row.
        """
        return self._execute(sql, args, FETCH_ONE, dict_cursor)

    def fetch_all_rows(self, sql, args=None, dict_cursor=False):
        """
        Execute a select statement and fetch all rows
        """
        return self._execute(sql, args, FETCH_ALL, dict_cursor)

    def modify_rows(self, sql, args=None):
        """
        Execute an insert, update or delete statement.
        """
        # result =  self._execute(sql, args, MODIFY)
        # if self.auto_commit is True:
        #     self.connection.commit()
        return self._execute(sql, args, MODIFY)

        # return result

    def streaming_cursor(self, sql, args=None):
        """
        Generator function that executes a server side cursor.
        Minimize the burden of fetchall in a query that might return a large volume

        :param cursor_name: A string representing the name passed to the server side cursor
        :param sql: A string representing the sql statment to be executed
        :param args: A dictionary or sequence representing the arguments passed to the sql statement
        """

        with self.connection as cxn:
            with cxn.cursor() as cursor:
                cursor.arraysize = 3000
                log.debug(cursor.mogrify(sql, args))
                cursor.execute(sql, args)
                while True:
                    result_set = cursor.fetchmany()
                    if not result_set:
                        break
                    for row in result_set:
                        yield row

    def roll_back(self):
        self.cursor.rollback()

    def close(self):
        self.connection.close()

    def copy_edit_from_file(self, sql_input, copy_file):
        """
        This copy function provides user a powerful tool to copy data to database
        :param sql_input is query to specify column and DELIMITER
        :param copy_file: file with data with correct DELIMITER in the query

        example:
        copy geo.gps
        (user_id, puc_id, timeoffix_seconds, timeoffix_millis,
        ts, isvalid, latitude, longitude, groundspeed, truecourse, magneticvariation,
        altitude, satellites) from stdin WITH DELIMITER E'\t';
        """

        if not os.path.exists(copy_file):
            raise ValueError("File: {} does not exist".format(copy_file))
        log.info("copy from file: {}".format(copy_file))
        log.debug("""executing copy cursor""")
        log.debug("""sql to be executed: {} \nwith: {}""".format(sql_input, copy_file))

        with open(copy_file, 'r') as copy_file_buff:
            self.cursor.copy_expert(sql_input, copy_file_buff)
        self.connection.commit()

    def copy_table_to_file(self, table_name, dump_file_path):
        """
        copy table to file
        :param file_path:
        :return:
        """
        with self.connection.cursor() as cursor:
            with open(dump_file_path, 'w') as dump_file:
                cursor.copy_to(dump_file, table_name)
        return dump_file_path

    def copy_table_from_file(self, table_name, dump_file_path):
        """
        copy table to file
        :param file_path:
        :return:
        """

        with self.connection.cursor() as cursor:
            with open(dump_file_path, 'r') as dump_file:
                cursor.copy_from(dump_file, table_name)
        self.connection.commit()

    def gp_copy_schema_table(self, table_name, db):
        create_table_sql = '''
        create table {table_name} (
        {col_and_datatype}
        )
        '''

        schema, table = self.break_schema_table_name(table_name)

        col_and_datatype = self.get_col_and_data_type_str(db, schema, table)

        create_table_sql = create_table_sql.format(table_name=table_name,
                                                   col_and_datatype=col_and_datatype)
        try:
            # print (create_table_sql)
            self.cursor.execute(create_table_sql)
        except Exception as error:
            print(error)
            self.roll_back()
        finally:
            self.connection.commit()

    def get_col_and_data_type_str(self, db, schema, table):
        table_schema_info = db.fetch_all_rows('''
            select column_name, data_type
                from
                      information_schema.columns
                where
                      table_schema = '{schema}'
                      and table_name = '{table}'
        '''.format(schema=schema,
                   table=table))
        col_data = []
        for row in table_schema_info.query_data:
            col_data.append("{} {}".format(row.column_name,
                                           row.data_type))
        col_and_datatype = ", \n".join(col_data)
        return col_and_datatype

    def break_schema_table_name(self, table_name):
        if "." in table_name:
            schema, table = table_name.split('.')
        else:
            schema = 'public'
            table = table_name
        return schema, table

    @staticmethod
    def load_sql(sql_file):
        sql = ''
        with open(sql_file, 'r') as f:
            for line in f.read():
                sql += line
        return sql

    def __str__(self):
        return "database: {database} - User: {user} - Host: {host} - Port: {port}".format(**self.config)


def copy_table_fromdb_todb(table_name, from_db, to_db):
    """
    This function is used to copy across databases
    :param table_name:
    :param from_db: dictionary
    :param to_db: dictionary

    """

    temp_file_name = "/tmp/{}.txt".format(table_name)
    log.info("copy_file_dump: {}".format(temp_file_name))

    from_db_conn = PgsqlExecutor(from_db)
    from_db_conn.copy_table_to_file(table_name, temp_file_name)
    to_db_conn = PgsqlExecutor(to_db)
    to_db_conn.copy_table_from_file(table_name, temp_file_name)

    from_db_conn.close()
    to_db_conn.close()
