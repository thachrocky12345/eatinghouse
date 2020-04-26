import simplejson
from aiohttp import web, MultipartReader
from main.auth import authenticate
# from zope.interface import implementer
from main.decorator import json_format_result
from endpoint import base


def get_endpoints(app, mapper_business):
    @authenticate
    async def get_url_request(request):
        username = base.get_request_arg(request, 'username')
        id = base.get_request_arg(request, 'id')
        dto = json_format_result(mapper_business.get_dto())
        if id:
            dto = mapper_business.get_dto(id=1)
            dto = await mapper_business.select_business(dto)
        elif username:
            dto = mapper_business.get_dto(username=username)
            dto = await mapper_business.select_business(dto, "username")
        await app.ai_client.flush()
        return web.Response(text=dto, status=200)

    @authenticate
    async def get_options(request):
        dto = mapper_business.get_properties()
        return web.Response(text=json_format_result(dto), status=200)

    app.router.add_route('GET', '/business', get_url_request)
    app.router.add_route('OPTIONS', '/business', get_options)



