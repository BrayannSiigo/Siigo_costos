import json
from azure.identity import AzureCliCredential,ClientSecretCredential
from resourses_management.azure_fetcher import AzureFetcherMain
from resourses_management.azure_fetchers import SubscriptionFetcher,ResourceFetcher,ResourceGroupsFetcher,TagsProcessor
from resourses_management.ids import RandomIdMaker
from core import settings
class AzureFetcherFactory():
    def build( self,**kwargs):

        credentials = self._make_credentials()
        fetchers = {}
        fetchers['subscriptions_fetcher'] = SubscriptionFetcher(id_factory=RandomIdMaker(length=4),credentials=credentials)
        fetchers['resource_groups_fetcher'] =  ResourceGroupsFetcher(RandomIdMaker(length=8),credentials,TagsProcessor())
        fetchers['resources_fetcher']  =  ResourceFetcher(RandomIdMaker(length=12),credentials)
        return AzureFetcherMain(fetchers,**kwargs)
    def _load_credentials_from_file(self,az_credentials_file):
        try:
            credentials_dic =  json.load(open(az_credentials_file))
        except Exception as e:
            raise Exception(f'Credentials file does not exist.')

        tenant_id = credentials_dic.get('tenant','')
        client_id =  credentials_dic.get('appId','')
        client_secret =  credentials_dic.get('password','')
        return {"tenant_id":tenant_id,"client_id":client_id,"client_secret":client_secret}
    def _make_az_credentials(self,):
        credentials_dic = None
        if settings.az_auth_method == "TOKEN":
            serice_principal = self._load_credentials_from_file(settings.az_credentials_file)
            if settings.az_manager_type == 'API':
                credentials = ServicePrincipalAPIAuth( **serice_principal )
            else:
                credential = ClientSecretCredential(**serice_principal)
        elif  settings.az_manager_type == 'SDK':
            credentials = AzureCliCredential()
        else:
            raise Exception('CLI Auth is not allowed with Azure API, please use TOKEN Auth.')
        return credentials
    def _make_api_credentials(self,az_auth_method,az_credentials_file):
        if az_auth_method == "TOKEN":
            serice_principal = self._load_credentials_from_file(az_credentials_file)
            credentials = ServicePrincipalAPIAuth( **serice_principal )
        else:
            raise Exception('CLI Auth is not allowed with Azure API, please use TOKEN Auth.')
        return credentials
    def _make_sdk_credentials(self,az_auth_method,az_credentials_file):
        if az_auth_method == "TOKEN":
            serice_principal = self._load_credentials_from_file(az_credentials_file)
            settings.logger.debug(serice_principal)
            credentials = ClientSecretCredential(**serice_principal)
        else:
            credentials = AzureCliCredential()
        return credentials
    def _make_credentials(self):
        if settings.az_manager_type == "SDK":
            credentials =  self._make_sdk_credentials(settings.az_auth_method,settings.az_credentials_file)
            # azure_manager = SDKResourceGroupManager(credentials,subscription_id)
        elif settings.az_manager_type == "API":
            credentials =  self._make_api_credentials(settings.az_auth_method,settings.az_credentials_file)
            # azure_manager = APIResourceGroupManager(credentials,subscription_id)
        else:
            raise Exception('Unrecognized Azure Manager Type, Allowed values are [SDK,API] .')

        return credentials
