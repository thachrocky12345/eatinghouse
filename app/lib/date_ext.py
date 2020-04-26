from datetime import datetime, timedelta
import psycopg2
import dateutil.parser
from pytz import timezone, utc


def datetime_range(from_date, to_date, interval=timedelta(days=1), include_end_date=True):
    if not isinstance(from_date, datetime):
        raise TypeError("from_date argument must be a datetime, not {0}".format(type(from_date)))
    if not isinstance(to_date, datetime):
        raise TypeError("to_date argument must be a datetime, not {0}".format(type(to_date)))
    if not isinstance(interval, timedelta):
        raise TypeError("interval argument must be a timedelta, not {0}".format(type(interval)))
    if interval == timedelta(0):
        raise ValueError("interval must not be 0")

    if include_end_date is False:
        to_date = to_date - timedelta(days=1)

    while from_date >= to_date and interval < timedelta(0):
        if from_date == to_date and include_end_date is False:
            raise StopIteration
        yield from_date
        from_date = from_date + interval

    while from_date <= to_date and interval > timedelta(0):
        if from_date == to_date and include_end_date is False:
            raise StopIteration
        yield from_date
        from_date = from_date + interval


def totalseconds(datetime_ts):
    if not isinstance(datetime_ts, timedelta):
        raise TypeError("datetime_ts argument must be a timedelta, not {0}".format(type(datetime_ts)))
    return (datetime_ts.microseconds + (datetime_ts.seconds + datetime_ts.days * 24 * 3600) * 1000000) / 1000000  # python 2.7 can use the total seconds method


def timestamp(datetime_ts):
    datetime_ts = datetimetz(datetime_ts)
    d = datetime_ts - datetimetz(datetime(1970, 1, 1))
    return totalseconds(d)




def is_weekdays(current_time):
    if 1 <= current_time.isoweekday() <= 5:
        return True
    return False


def now():
    return datetime.now()


def min():
    return datetimetz(datetime.min)


def datetimetz_round(datetimetz, round_to=timedelta(minutes=1), round_func=round):
    round_to = totalseconds(round_to)
    t = timestamp(datetimetz)
    t = int(round_to * round_func(float(t) / round_to))
    return utcfromtimestamp(t)


def utcfromtimestamp(timestamp):
    d = datetime.utcfromtimestamp(timestamp)
    return datetimetz(d)


def get_tz(datetime_timedelta):
    total = totalseconds(datetime_timedelta)
    return psycopg2.tz.FixedOffsetTimezone(total / 60)


def datetimetz(datetime_stamp, tz=psycopg2.tz.FixedOffsetTimezone()):
    return datetime_stamp.replace(tzinfo=tz)


def parse_isoformat(string):
    timestamp = dateutil.parser.parse(string)
    try:
        old_tz_info = timestamp.tzinfo.utcoffset(None)
    except:
        old_tz_info = timedelta(0)

    new_tz_info = psycopg2.tz.FixedOffsetTimezone(totalseconds(old_tz_info) / 60)
    timestamp = timestamp.replace(tzinfo=new_tz_info)
    return timestamp


def get_market_time():
    current = datetime.utcnow().replace(tzinfo=utc)
    return current.astimezone(timezone('America/New_York'))

def is_market_open():
    current_time = get_market_time()
    return 9 * 60 + 30 <= current_time.hour * 60 + current_time.minute <= 16 * 60

def get_utc():
    return datetime.utcnow().replace(tzinfo=utc)

