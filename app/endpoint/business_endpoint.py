import simplejson
from aiohttp import web, MultipartReader
from main.auth import authenticate
# from zope.interface import implementer
from main.decorator import json_format_result
from endpoint import base
from lib.hash_password import hash_password

def get_endpoints(app, mapper_business):
    @authenticate
    async def get_business_request(request):
        # print(request.business)
        username = base.get_request_arg(request, 'username')
        id = base.get_request_arg(request, 'id')
        dto = json_format_result(mapper_business.get_dto())
        if id:
            dto = mapper_business.get_dto(id=id)
            dto = await mapper_business.select_business(dto)
        elif username:
            dto = mapper_business.get_dto(username=username)
            dto = await mapper_business.select_business(dto, "username")
        await app.ai_client.flush()
        return web.Response(text=json_format_result(dto), status=200)

    @authenticate
    async def insert_new_business(request):
        if request.business:
            if request.business.id != 1:
                return web.Response(text="Not authorized", status=401)
        json_data = await request.json()
        if json_data.get("id"):
            del json_data["id"]
        if json_data.get("created"):
            del json_data["created"]
        # print(json_data)
        dto = mapper_business.get_dto(**json_data)
        dto = await mapper_business.insert_business(dto)
        await app.ai_client.flush()
        return web.Response(text=json_format_result(dto), status=200)

    @authenticate
    async def get_options(request):
        dto = mapper_business.get_properties()
        return web.Response(text=json_format_result(dto), status=200)

    @authenticate
    async def update_business(request):
        if request.business:
            if request.business.id != 1:
                return web.Response(text="Not authorized", status=401)
        json_data = await request.json()
        if json_data.get("created"):
            del json_data["created"]

        if json_data.get("id"):
            dto = mapper_business.get_dto(id=int(json_data.get("id")))
            dto = await mapper_business.select_business(dto)
            await adjust_hash_password(dto, json_data)
            dto = await mapper_business.update_business(dto, where_column="id")
        elif json_data.get("username"):
            dto = mapper_business.get_dto(username=json_data.get("username"))
            dto = await mapper_business.select_business(dto, where_column="username")
            print(dto)
            if not dto:
                return web.Response(text="not sufficient data", status=500)

            await adjust_hash_password(dto, json_data)
            dto = await mapper_business.update_business(dto, where_column="username")

        else:
            return web.Response(text="not sufficient data", status=500)

        await app.ai_client.flush()
        return web.Response(text=json_format_result(dto), status=200)

    async def adjust_hash_password(dto, json_data):
        for key, val in json_data.items():
            if key == "password":
                dto.password = hash_password(val.encode('utf-8'))
            else:
                dto.update_attr(key, val)

    app.router.add_route('GET', '/business', get_business_request)
    app.router.add_route('POST', '/business', insert_new_business)
    app.router.add_route('PUT', '/business', update_business)
    app.router.add_route('OPTIONS', '/business', get_options)



