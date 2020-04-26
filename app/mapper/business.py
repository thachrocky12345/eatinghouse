from mapper.base import Mapper, Dto, DtoInteger, DtoText, DtoBoolean, DtoTimestamp, DtoObject, BulkVar, BulkList
import hashlib
from main.decorator import json_format_result

def hash_password(password):
    return hashlib.sha1(password).hexdigest()

class DtoBusiness(Dto):
    def __init__(self, **kwargs):
        Dto.__init__(self)
        self.add_attr('id', DtoInteger(), nullable=False, has_default=True, read_only=True)
        self.add_attr('username', DtoText(), nullable=False, has_default=False)
        self.add_attr('password', DtoText(), nullable=False, has_default=False)
        self.add_attr('email', DtoText(), nullable=False, has_default=False)
        self.add_attr('phone', DtoText(), nullable=True, has_default=False)
        self.add_attr('full_address', DtoText(), nullable=True, has_default=False)
        self.add_attr('hash_recovery', DtoText(), nullable=True, has_default=False)
        self.add_attr('business_role', DtoText(), nullable=True, has_default=False)
        self.add_attr('business_owner_id', DtoInteger(), nullable=False, has_default=True, read_only=True)
        self.add_attr('created', DtoTimestamp(), nullable=False, has_default=True)
        self.update(**kwargs)


class MapperBusiness(Mapper):
    def __init__(self, db):
        Mapper.__init__(self, db, DtoBusiness, 'option.business')

    async def select_business(self, dto, where_column='id'):
        dto = await self.select(dto, where_column)
        dto = json_format_result(dto)
        return dto





