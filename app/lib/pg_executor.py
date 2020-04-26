import asyncio
import aioodbc
import pyodbc
import logging
import time
import asyncpg
import re
import traceback
import os

from collections import namedtuple


class NullHandler(logging.Handler):
    def emit(self, record):
        pass

# Execution types
FETCH_ONE = 'fetchone'
FETCH_ALL = 'fetchall'
MODIFY = 'modify'

log = logging.getLogger('PGConnector')
log.addHandler(NullHandler())

ExecutionResults = namedtuple('ExecutionResults', ['query_data', 'columns'])

RECONNECT_ATTEMPTS = 5


class PGExecutor(object):
    """
    Async postgres for python3.7
    """

    def __init__(self, username, password, host, port='5432', database='posrgres'):

        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.database = database.lower()
        self.connect = None

    async def get_connection(self):
        for retry in range(RECONNECT_ATTEMPTS, -1, -1):
            try:
                if self.connect is not None and not self.connect.is_closed():
                    return self.connect
                loop = asyncio.get_event_loop()
                self.connect = await asyncpg.connect(database=self.database.lower(),
                                                user=self.username,
                                                password=self.password,
                                                host=self.host,
                                                port=self.port,
                                                loop=loop)
                return self.connect
            except Exception as error:
                print('Error Occured at Database Connection Creation {}'.format(error))
                print(self.database, self.host)
                if retry == 0:
                    raise
                log.warning('Could not connect to Database. Will retry in 10 seconds. {} retries left'.format(retry))
                time.sleep(1)

    async def _execute(self, sql, args=None, execution_type=MODIFY):
        """
        Execute a select statement and fetch a single row.
        query_data return nametupled

        reconnect if connect is closed() or self.connection.closed != 0
        """
        # if self.connection_pool is None or self.connection_pool.closed:

        connect = await self.get_connection()

        if args:
            query_data = await connect.fetch(sql, *args)
        else:
            query_data = await connect.fetch(sql)
        if execution_type == FETCH_ONE:
            query_data = query_data[0]
        elif execution_type == FETCH_ALL:
            pass
        else:
            query_data = None
        return query_data

    async def fetch_one_row(self, sql, args=None):
        """
        Execute a select statement and fetch a single row.
        """
        data = await self._execute(sql, args, FETCH_ONE)
        return data

    async def fetch_all_rows(self, sql, args=None):
        """
        Execute a select statement and fetch all rows
        """
        data = await self._execute(sql, args, FETCH_ALL)
        return data

    async def insert_many(self, sql, data_list):
        """
        await rest.insert_many("insert into test values ($1, $2)", [("c", 3) , ("d", 4)])
        :param sql: "insert into test values ($1, $2)"
        :param data_list: [("c", 3) , ("d", 4)]
        :return:
        """
        connect = await self.get_connection()
        await connect.executemany(sql, data_list)

    async def modify_rows(self, sql, args=None):
        """
        Execute an insert, update or delete statement.
        """
        connect = await self.get_connection()

        if args:
            data = await connect.execute(sql, *args)
        else:
            data = await connect.execute(sql)
        return data

    async def copy_to_file(self, sql, file, args=[]):
        connect = await self.get_connection()
        result = await connect.copy_from_query(
         sql, *args, output=file, format='csv')
        print(result)

    async def copy_from_file(self, table_name, schema_name, file):
        connect = await self.get_connection()
        with open(file, 'rb') as f:
            result = await connect.copy_to_table(
            table_name.lower(), schema_name=schema_name.lower(), source=f, format='csv', header=True)
        print(result)
        os.remove(file)



if __name__ == '__main__':
    from datetime import datetime

    async def run():
        start_time = datetime.now()
        rest = PGExecutor(username='meepdb', password='Awesome12345@!',
                          host='104.248.3.71', database='V3_Central')
        #data = await rest.fetch_all_rows("select * from test")
        # data = await rest.insert_many("insert into test values ($1, $2)", (("c", 3) , ("d", 4)))
        # data = await rest.fetch_all_rows("select * from test where number = $1", [4])
        # print(data)
        # data = await rest.copy_to_file("select * from test where name in ($1, $2)", ('c', 'd'), file="../test.csv")
        # await rest.modify_rows("delete from test")
        data = await rest.copy_from_file(table_name="log",
                                         file='C:\\Vertical Merge\\SQL2PostGes\\backup\\V3_Central\\dbo_Log.bak')
        data = await rest.fetch_all_rows("select *  from dbo.log")
        print(data)
        print("run_time: {}".format(datetime.now() - start_time))

    asyncio.get_event_loop().run_until_complete(run())
