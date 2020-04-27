import simplejson
from aiohttp import web, MultipartReader
from main.auth import authenticate
from main.decorator import json_format_result
from endpoint import base
from lib.hash_password import hash_password
from datetime import datetime

def get_endpoints(app, mapper_business, mapper_session):
    print("token process")
    async def get_session_request(request):
        json_data = await request.json()
        # print(json_data)
        token_dto = mapper_session.get_dto(business_id=1)
        username = json_data["username"]
        password = json_data["password"]
        if username:
            business_dto = mapper_business.get_dto(username=username)
            business_dto = await mapper_business.select_business(business_dto, where_column="username")
            if hash_password(password.encode('utf-8')) == business_dto.password:
                token_dto = mapper_session.get_dto(business_id=business_dto.id)
                token_dto = await mapper_session.select_token(token_dto, 'business_id')
                if not token_dto:
                    # insert
                    token_dto = mapper_session.get_dto(modified=datetime.now(), lifetime=60 * 60 * 24 * 30, business_id=business_dto.id)  # 1 month
                    token_dto = await mapper_session.insert_token(token_dto)
                # print("correct", token_dto)
        await app.ai_client.flush()
        return web.Response(text=json_format_result(token_dto), status=200)

    async def add_token(request):
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
    async def get_options(request):
        dto = mapper_session.get_properties()
        return web.Response(text=json_format_result(dto), status=200)

    app.router.add_route('PUT', '/token', add_token)
    app.router.add_route('POST', '/token', get_session_request)
    app.router.add_route('OPTIONS', '/token', get_options)



