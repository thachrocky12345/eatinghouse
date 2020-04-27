import simplejson
from aiohttp import web, MultipartReader
from asynapplicationinsights.aiohttp import use_application_insights
from main.config import instrumentation_key, db, static_db
import mapper.business
import mapper.session

import endpoint.business_endpoint
import endpoint.session_endpoint




mapper_business = mapper.business.MapperBusiness(db, static_db)
mapper_session = mapper.session.MapperSession(db, static_db, mapper_business)




app = web.Application()
use_application_insights(app, instrumentation_key=instrumentation_key)

endpoint.business_endpoint.get_endpoints(app, mapper_business)
endpoint.session_endpoint.get_endpoints(app, mapper_business, mapper_session)
web.run_app(app, host='0.0.0.0', port=6789)