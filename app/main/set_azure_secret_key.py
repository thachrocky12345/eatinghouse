from applicationinsights import TelemetryClient
from azure.keyvault import KeyVaultClient, KeyVaultAuthentication
from azure.common.credentials import ServicePrincipalCredentials
import os
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

import sys

key_vault_uri = "https://thachstockservers.vault.azure.net/"
instrumentation_key = "08121580-af3e-4f86-8680-3a83210704b0"

tc = TelemetryClient(instrumentation_key)

credential = DefaultAzureCredential()

keyVaultName = os.environ["KEY_VAULT_NAME"]

KVUri = "https://" + keyVaultName + ".vault.azure.net"

client = SecretClient(vault_url=KVUri, credential=credential)
print (client.get_secret("test").value)

# client.set_secret("DNSHostEH", "136.34.106.138")
# client.set_secret("TokenEH", "TheEatingHouseThachVu12345!")

print(" done.")