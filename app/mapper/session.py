from base import Mapper, Dto, DtoText, DtoInteger, DtoBoolean, DtoTimestamp
from datetime import datetime
from lib import date_ext
from lib.db.pgdb import BulkVar
from twisted.internet import defer
import string
import random


class DtoSession(Dto):
    def __init__(self, **kwargs):
        Dto.__init__(self)
        self.add_attr('id', DtoText(), nullable=False, has_default=False)
        self.add_attr('modified', DtoTimestamp(), nullable=True, has_default=False)
        self.add_attr('lifetime', DtoInteger(), nullable=True, has_default=False)
        self.add_attr('data', DtoText(), nullable=True, has_default=False)
        self.add_attr('business_id', DtoInteger(), nullable=True, has_default=False)
        self.add_attr('set_cookie', DtoBoolean(), nullable=False, has_default=True, value=False)
        self.update(**kwargs)


class MapperSession(Mapper):
    def __init__(self, db):
        Mapper.__init__(self, db, DtoSession, 'option.session')

    @defer.inlineCallbacks
    def convert_to_user(self, dto):
        if dto.modified:
            dto.modified = datetime.fromtimestamp(dto.modified)
            dto.modified = date_ext.datetimetz(dto.modified)
        dto = yield Mapper.convert_to_user(self, dto)
        defer.returnValue(dto)

    @defer.inlineCallbacks
    def convert_to_db(self, dto):
        dto = yield Mapper.convert_to_db(self, dto)
        if dto.modified:
            dto.modified = date_ext.timestamp(dto.modified)
        del dto.set_cookie
        defer.returnValue(dto)

    def get_next_seq_id(self):
        ascii_set = string.ascii_uppercase + string.ascii_lowercase
        token = ''.join(random.sample(ascii_set, 32))
        return token

    @defer.inlineCallbacks
    def select(self, dto, where_column='id'):

        dto = yield self.convert_to_db(dto)
        sql_builder = self.get_sql_builder(dto)
        select_list, select_arg_list = sql_builder.get_select()

        sql = """SELECT {0} FROM {1} WHERE {2} IN (%b)""".format(select_list, self.table_name, where_column)
        sql_args = select_arg_list + (BulkVar(dto.get_attr(where_column), column=where_column),)

        db_return = yield self.bulk_db.cache_sql_fetchone(sql, *sql_args, read_only=True)
        if not db_return:
            db_return = yield self.bulk_db.execute_sql_fetchall(sql, *sql_args, read_only=True)

        dto = self.get_dto()
        dto.update_no_check(**db_return)
        dto = yield self.convert_to_user(dto)
        defer.returnValue(dto)
