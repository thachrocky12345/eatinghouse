from mapper.base import Mapper, Dto, DtoInteger, DtoText, DtoBoolean, DtoTimestamp, DtoObject, BulkVar, BulkList
import hashlib
from main.decorator import json_format_result
from datetime import datetime

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
        start_time = datetime.now()
        dto = await self.select(dto, where_column)
        print("run time: 0:00:00.06 {}".format(datetime.now() - start_time))
        dto = json_format_result(dto)
        return dto





