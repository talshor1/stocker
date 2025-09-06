from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from typing import Optional

class KeyVaultClient:
    _instance: Optional['KeyVaultClient'] = None
    _initialized: bool = False
    
    def __new__(cls, key_vault_name: str = "stocks-kv"):
        if cls._instance is None:
            cls._instance = super(KeyVaultClient, cls).__new__(cls)
        return cls._instance
    
    def __init__(self, key_vault_name: str = "stocks-kv"):
        if not self._initialized:
            self.key_vault_name = key_vault_name
            self.key_vault_uri = f"https://{key_vault_name}.vault.azure.net/"
            self.credential = DefaultAzureCredential()
            self.client = SecretClient(vault_url=self.key_vault_uri, credential=self.credential)
            self._initialized = True
    
    def get_secret(self, secret_name: str) -> str:
        try:
            retrieved_secret = self.client.get_secret(secret_name)
            return retrieved_secret.value
        except Exception as e:
            raise Exception(f"Failed to retrieve secret '{secret_name}': {e}")
