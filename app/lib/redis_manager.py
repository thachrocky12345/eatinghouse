import redis
import logging
import simplejson as json

from sbt_common import SingletonCommonManager, SbtGlobalCommon

logger = logging.getLogger(__name__)
_stocks_cache_exp_seconds = 23 * 60 * 60  # 23 hours in seconds


class RedisManager(object, metaclass=SingletonCommonManager):

    def __init__(self, host=None,
                 port=None,
                 max_conn=None):
        """
        Manages the connection to Redis and all related functions.

        Args:
          host (str): The hostname of the Redis cluster.
          port (port): The port of the Redis cluster.
        """
        cache_host = None
        cache_port = None
        self.__cache_pool = None
        self.__cache_client = None

        # exchangesymbol mapping
        self.EXCHANGE_MAPPING = {
            'Nasdaq': 'NSDQ',
            'NASDAQ': 'NSDQ',
            'NasdaqGS': 'NSDQ',
            'NasdaqGM': 'NSDQ',
            'NasdaqCM': 'NSDQCM',
            'ARCA': 'NYSEARCA',
        }

        self.SB_EXCHANGE_MAPPING = {
            'TSX': 'TO',
            'LSE': 'L',
            'AIM': 'L',
            'HK': 'HK',
            'TSXV': 'V',
            'PAR': 'PA',
            'STO': 'ST',
            'BSE': 'BR',
            'JPX': 'T',
            'CSE': 'CO',
            'FWB': 'DE',
            'SIX': 'SW',
            'MCX': 'ME',
            'MCE': 'MC',
            'ATH': 'AT',
            'MIL': 'MI',
        }

        config = SbtGlobalCommon.get_sbt_config()
        if not all([host, port]):
            host = config['redis']['host']
            port = config['redis']['port']

        if not max_conn:
            max_conn = config['redis'].get('max_connections', 100)

        logger.info('Redis Max Connections : ' + str(max_conn))

        if 'cache_host' in config['redis']:
            cache_host = config['redis']['cache_host']

        if 'cache_port' in config['redis']:
            cache_port = config['redis']['cache_port']

        cache_max_connections = config['redis'].get(
            'cache_max_connections', 100)

        self.__pool = redis.ConnectionPool(max_connections=max_conn,
                                           host=host, port=port)
        self.__client = redis.StrictRedis(connection_pool=self.__pool)

        if cache_host and cache_port and \
                SbtGlobalCommon.supported_redis_host():
            try:
                logger.info('Redis Max Cached Connections : ' + str(cache_max_connections))
                self.__cache_pool = redis.ConnectionPool(
                    max_connections=cache_max_connections,
                    host=cache_host,
                    port=cache_port)
                self.__cache_client = redis.StrictRedis(
                    connection_pool=self.__cache_pool)
                self.__cache_client.ping()
            except Exception as e:
                self.__cache_pool = None
                self.__cache_client = None
                logger.error("Unable to initialize REDIS cache." + str(e))
        else:
            logger.error("Unsupported or cache config entries are missing.")

    def delete(self, *names):
        if self.__cache_client:
            return self.__cache_client.delete(names)
        else:
            return 0

    def get_cache_keys(self, pattern):
        if self.__cache_client:
            return self.__cache_client.keys(pattern)
        else:
            return []

    def remaining_seconds(self, name):
        if self.__cache_client:
            return self.__cache_client.ttl(name)
        else:
            return -100

    def cache_key_exists(self, name):
        return self.get_value(name) is not None

    def delete_key(self, name):
        if self.__cache_client:
            return self.__cache_client.delete(name)
        else:
            return None

    def get_value(self, name):
        if self.__cache_client:
            return self.__cache_client.get(name)
        else:
            return None

    def get_string_value(self, name):
        stored_data = None

        if self.__cache_client:
            data = self.__cache_client.get(name)

            if not data:
                return stored_data

            if isinstance(data, bytes):
                stored_data = data.decode('utf-8')
            else:
                stored_data = data

        return stored_data

    def set(self, key, value, ttl=600):
        try:
            value = json.dumps(value)
        except:
            logger.info("Values should not be passed as json" + key)

        if self.__cache_client:
            return self.__cache_client.set(key, value, ttl)
        else:
            return False

    def get(self, key):
        if self.__cache_client:
            cache_item = self.__cache_client.get(key)
            if cache_item:
                try:
                    return json.loads(cache_item)
                except:
                    return cache_item
            else:
                return None

        return None

    def set_value(self, name, value, ex=None,
                  px=None, nx=False, xx=False):
        if self.__cache_client:
            return self.__cache_client.set(name, value, ex, px,
                                           nx, xx)
        else:
            return False

    def publish(self, channel, message):
        """
        Publish a message to a Redis channel.

        Args:
          channel (str): The channel to send on.
          message (str): The message to send.
        """
        logger.info("Publishing to the REDIS channel: " + channel)
        self.__client.publish(channel=channel, message=message)

    def pubsub(self, ignore_subscribe_messages=True):
        return self.__client.pubsub(
            ignore_subscribe_messages=ignore_subscribe_messages)

    def stream_article(self, channel_prefix, article):
        """
        Stream a Stansberry Article to channels with a specified prefix.

        Args:
          channel_prefix (str): The prefix to the channel to stream to.
          article (dict): The article to stream.
        """
        if article and 'tickers' in article:
            for ticker in article['tickers']:
                channel = channel_prefix + ticker
                self.publish(channel=channel, message=json.dumps(article))

        self.publish(channel=channel_prefix + "*", message=json.dumps(article))

    def process_stock_cache(self, stock_key, cache_data):
        if self.key_exists(stock_key):
            self.set_lpush(stock_key, cache_data)
        else:
            self.set_lpush(stock_key, cache_data)
            self.set_expire(stock_key, _stocks_cache_exp_seconds)

    def key_exists(self, key):
        return self.__cache_client.exists(key)

    def set_expire(self, key, time):
        return self.__cache_client.expire(key, time)

    def set_lpush(self, key, value):
        return self.__cache_client.lpush(key, value)

    def get_lrange(self, key, start, end):
        stored_data = []
        data_list = self.__cache_client.lrange(key, start, end)

        for data in data_list:
            try:
              stored_data.append(json.loads(data.decode('utf-8')))
            except json.scanner.JSONDecodeError:
              stored_data.append(eval(data.decode('utf-8')))
            except Exception as e:
              self._logger.error("Unable to decode:" +  data)

        return stored_data

    def ping_cache(self):
        """
        Ping the Redis server to test the connection.
        Returns
            str: The response from pinging the Redis server.

        """
        if self.__cache_client:
            return self.__cache_client.ping()
        else:
            return False

    def ping(self):
        """
        Ping the Redis server to test the connection.
        Returns
            str: The response from pinging the Redis server.

        """
        return self.__client.ping()

    def disconnect(self):
        """
        Disconnect all connections in the pool
        """
        self.__pool.disconnect()

    def client_kill(self, address):
        """
        Disconnect the client at address.
        Args:
          address(str): The address (ip:port) to disconnect.
        """
        self.__client.client_kill(address)

    def get_exchange_mapping(self, exchange_symbol, sb_map=False):
        if exchange_symbol is not None:
            if exchange_symbol.startswith('OTC'):
                exchange_mapping = 'OTC'
            else:
                exchange_mapping = \
                    self.EXCHANGE_MAPPING.get(exchange_symbol, exchange_symbol) \
                        if not sb_map else \
                    self.SB_EXCHANGE_MAPPING.get(exchange_symbol, exchange_symbol)
        else:
            self._logger.error(" EXCHANGE IS NONE.")
            exchange_mapping = exchange_symbol

        return exchange_mapping
