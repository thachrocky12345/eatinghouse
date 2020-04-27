import re
from functools import wraps
from main.config import tc, AUTH_TOKEN, db, static_db
import sys
import traceback
from aiohttp import web
import json
from mapper.business import MapperBusiness
from lib.pg_executor import PGExecutor
import base64
from lib.hash_password import hash_password


mapper_business = MapperBusiness(db, static_db)


async def get_authenticate(request):
    print(request.headers)
    if 'Authorization' in request.headers.keys():
        request.business = None
        access_token = request.headers['Authorization']
        access_token = re.sub('Bearer', '', access_token)
        if access_token.strip() == AUTH_TOKEN:
            business_dto = mapper_business.get_dto(id=1)
            request.business = await mapper_business.select_business(business_dto)
            return 'valid'
        access_token = re.sub("Basic", "", request.headers['Authorization']).strip()
        message = access_token.encode('utf-8')
        base64_bytes = base64.b64decode(message)
        username, password = base64_bytes.split(b':')

        business_dto = mapper_business.get_dto(username=username.decode('utf-8'))
        business_dto = await mapper_business.select_business(business_dto, where_column='username')

        print(type(password), password)

        if hash_password(password) == business_dto.password:
            print("correct")
            request.business = business_dto
            return 'valid'
    return "invalid"


def authenticate(f):
    @wraps(f)
    async def wrapped(request, *args, **kwargs):

        auth = await get_authenticate(request)
        if auth != 'valid':
            return web.Response(text="request not allowed", status=404)
        try:
            value = await f(request, *args, **kwargs)
            return value
        except Exception as error:
            exc_info = sys.exc_info()
            traceback.print_exception(*exc_info)
            properties = {'error type': 'OCR api',
                          'type': str(traceback.format_exc(limit=3)),
                          "error": str(error),
                          'problem Id': 'OCR api {}'.format(f)}
            tc.track_exception(*sys.exc_info(), properties=properties)
            tc.flush()
            return web.Response(text=json.dumps(properties), status=500)

    return wrapped




