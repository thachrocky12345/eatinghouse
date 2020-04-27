from mapper.base import Mapper, Dto, DtoInteger, DtoText, DtoBoolean, DtoTimestamp, DtoObject, BulkVar, BulkList
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
        self.update(**kwargs)


class MapperSession(Mapper):
    def __init__(self, db, static_db, mapper_business):
        Mapper.__init__(self, db, DtoSession, 'option.session')
        self.static_db = static_db
        self.mapper_business = mapper_business

    async def get_next_seq_id(self):
        ascii_set = string.ascii_uppercase + string.ascii_lowercase
        token = ''.join(random.sample(ascii_set, 32))
        return token

    async def select_token(self, dto, where_column='id'):
        dto = await self.select(dto, where_column)
        return dto

    async def insert_token(self, dto):
        id_value = await self.get_next_seq_id()
        dto = await self.insert(dto, dto_id=id_value)
        return dto

