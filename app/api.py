import simplejson
from aiohttp import web, MultipartReader
from asynapplicationinsights.aiohttp import use_application_insights
from main.config import instrumentation_key, DSN_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT
import mapper.business

import endpoint.business_endpoint

from lib.static_db import PgsqlExecutor
from lib.pg_executor import PGExecutor

test_db = dict(
    host=DSN_HOST,
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    port=DB_PORT

)

static_db = PgsqlExecutor(test_db)
db = PGExecutor(username=DB_USER, password=DB_PASSWORD, host=DSN_HOST, port=DB_PORT, database=DB_NAME)
mapper_business = mapper.business.MapperBusiness(db)




app = web.Application()
use_application_insights(app, instrumentation_key=instrumentation_key)

endpoint.business_endpoint.get_endpoints(app, mapper_business)
web.run_app(app, host='0.0.0.0', port=6789)