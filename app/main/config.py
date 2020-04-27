from applicationinsights import TelemetryClient
import os
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
from lib.cache_data import CacheData
from lib.static_db import PgsqlExecutor
from lib.pg_executor import PGExecutor

import sys

key_vault_uri = "https://thachstockservers.vault.azure.net/"
instrumentation_key = "08121580-af3e-4f86-8680-3a83210704b0"

tc = TelemetryClient(instrumentation_key)

credential = DefaultAzureCredential()

keyVaultName = os.environ["KEY_VAULT_NAME"]

KVUri = "https://" + keyVaultName + ".vault.azure.net"

sql_cached = CacheData(max_cache_number=100000, cache_time=1000)


try :
    print('Loading Keys')
    client = SecretClient(vault_url=KVUri, credential=credential)
    print(client.get_secret("test").value)
    AUTH_TOKEN = client.get_secret("TokenEH").value
    DSN_HOST = client.get_secret("DNSHostEH").value
    DB_NAME = client.get_secret("DbNameEH").value
    DB_USER = client.get_secret("DbUserEH").value
    DB_PASSWORD = client.get_secret("DbPasswordEH").value
    DB_PORT = client.get_secret("DNSPortEH").value
    # print(AUTH_TOKEN)
except:
    print('Error Occured in Fetching Keys')
    print(*sys.exc_info())
    tc.track_exception(*sys.exc_info(), properties={"type": "Error Occured in Fetching Keys",
                                                    "function": "getKeys"})
    tc.flush()

test_db = dict(
    host=DSN_HOST,
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    port=DB_PORT

)

static_db = PgsqlExecutor(test_db)
db = PGExecutor(username=DB_USER, password=DB_PASSWORD, host=DSN_HOST, port=DB_PORT, database=DB_NAME)