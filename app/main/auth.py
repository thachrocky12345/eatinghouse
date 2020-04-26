import re
from functools import wraps
from main.config import tc, AUTH_TOKEN
import sys
import traceback
from aiohttp import web
import json


async def get_authenticate(request):
    if 'Authorization' in request.headers.keys():
        access_token = request.headers['Authorization']
        access_token = re.sub('Bearer', '', access_token)
        if access_token.strip() == AUTH_TOKEN:
            return 'valid'
    print("invalid")
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




