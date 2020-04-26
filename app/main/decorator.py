import simplejson as json
import csv
from io import StringIO, BytesIO
from functools import wraps
from datetime import datetime, date, timedelta
from mapper.base import Dto, DtoError, DtoType, Null
from lib import date_ext
from pytz import utc, timezone


def convert_timezone(o, user_timezone):
    tz = timezone(user_timezone)
    try:
        o = o.astimezone(tz)
    except:
        o = o.replace(tzinfo=utc)
        o = o.astimezone(tz)
    return o.isoformat()


class CustomEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, timedelta):
            return date_ext.totalseconds(o)
        if isinstance(o, datetime):
            return convert_timezone(o, 'America/New_York')
        if isinstance(o, date):
            return o.isoformat()
        if isinstance(o, Dto):
            return dict(o)
        if isinstance(o, DtoError):
            return o.__dict__
        if isinstance(o, DtoType):
            return str(o).lower()

        if isinstance(o, bytes):
            return str(o).encode('string_escape')

        if isinstance(o, Null):
            return None

        return json.JSONEncoder.default(self, o)


def convert_str_to_date(date_string):
    if len(date_string) < 4:
        raise TypeError()
    try:
        float(date_string)
    except (ValueError, TypeError):
        return date_ext.parse_isoformat(date_string)
    raise TypeError()  # if a number can be converted to a float then do not cast it into a date


class CustomDecoder(json.JSONDecoder):
    def decode_data(self, o):
        if isinstance(o, list):
            for index, element in enumerate(o):
                o[index] = self.decode_data(element)
            return o
        if isinstance(o, datetime):
            return o.isoformat()
        if isinstance(o, date):
            return o.isoformat()
        if isinstance(o, dict):
            for key in o:
                o[key] = self.decode_data(o[key])
            return o

        try:
            o = convert_str_to_date(o)
        except:
            pass
        return o

    def decode(self, s):
        o = json.JSONDecoder.decode(self, s)
        return self.decode_data(o)

#
# def geojson_format(f):
#     @wraps(f)
#     def wrapper(self, request, *args, **kwargs):
#         try:
#             content = geojson.load(request.content)
#             for feature in content.features:
#                 for prop in feature.properties:
#                     try:
#                         feature.properties[prop] = convert_str_to_date(feature.properties[prop])
#                     except:
#                         pass
#         except ValueError:
#             content = None
#         val = f(self, request, content, *args, **kwargs)
#         if isinstance(val, defer.Deferred):
#             val.addCallback(lambda result: geojson_format_result(result, request))
#             return val
#         return geojson_format_result(val, request)
#     return wrapper


def content_list(f):
    @wraps(f)
    def wrapper(self, request, content, *args, **kwargs):
        return_dict = False
        if not isinstance(content, list):
            return_dict = True
            content = [content]
        val = f(self, request, content, *args, **kwargs)
        if isinstance(val, defer.Deferred):
            val.addCallback(content_list_result, return_dict)
            return val
        return self.content_list_result(val, return_dict)
    return wrapper


def content_list_result(result, return_dict):
    if return_dict:
        try:
            result = result[0]
        except IndexError:
            result = {}
    return result

#
# def json_format(f):
#     @wraps(f)
#     def wrapper(self, request, *args, **kwargs):
#         try:
#             content = json.load(request.content, cls=CustomDecoder)
#         except ValueError:
#             content = None
#         val = f(self, request, content, *args, **kwargs)
#         if isinstance(val, defer.Deferred):
#             val.addCallback(json_format_result, request)
#             return val
#         return json_format_result(val, request)
#     return wrapper


def json_format_result(result):
    return json.dumps(result, cls=CustomEncoder, encoding='utf-8')


#
# def geojson_format_result(result, request):
#     request.setHeader('Content-Type', 'application/json')
#     for feature in result.features:
#         for prop in feature.properties:
#             if isinstance(feature.properties[prop], datetime) or isinstance(feature.properties[prop], date):
#                 feature.properties[prop] = feature.properties[prop].isoformat()
#             if isinstance(feature.properties[prop], uom.Uom):
#                 try:
#                     feature.properties[prop] = round(feature.properties[prop].base_value, 8)
#                 except:
#                     feature.properties[prop] = feature.properties[prop].base_value
#     return geojson.dumps(result)  # , cls=CustomEncoder, encoding='utf-8')


def csv_format(f):
    @wraps(f)
    def wrapper(self, request, *args, **kwargs):
        try:
            content = csv.reader(request.content)
        except ValueError:
            content = None
        val = f(self, request, content, *args, **kwargs)
        if isinstance(val, defer.Deferred):
            val.addCallback(csv_format_result, request)
            return val
        return csv_format_result(val, request)
    return wrapper


def csv_format_result(result, request):
    request.setHeader('Content-Type', 'text/csv')
    csv_file = StringIO()
    try:
        fieldnames = sorted(dict(result[0]).keys())
    except IndexError:
        return ''

    writer = csv.DictWriter(csv_file, fieldnames, quoting=csv.QUOTE_NONNUMERIC)
    writer.writeheader()
    for dto in result:
        writer.writerow(dict(dto))
    result = csv_file.getvalue()
    csv_file.close()
    return result
