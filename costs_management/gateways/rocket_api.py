# from urllib2 import Request, urlopen
import os
import urllib.parse
import json
import requests
from requests.auth import HTTPBasicAuth
from dict_json import save_obj,open_file
from core.settings import logger
from core import settings

DEFACULT_CUSTOMER_ID = os.environ['DEFACULT_CUSTOMER_ID']#

class RocketAPI:
    def __init__(self,token=None) -> None:
        self.base_url = "https://rocket.uat.enube.me/api/v1"
        self.token = token
       
        # print(self.token)
        self.auth_headers = {
        'Authorization': f'Bearer {self.token}'
        }    
    def authenticate(self):
        if self.token is not None:
            # self.token = self.authenticate().get('access_token',None) # TODO No authenticarse si no se necesita
            return
        clientID = os.environ['rocketClientId']
        clientSecret = os.environ['rocketClientSecret']
        # print(clientID)
        # print(clientSecret)
        response = requests.get(f'{self.base_url}/oauth2/token', auth = HTTPBasicAuth(clientID,clientSecret))
        if response.status_code == 200:
            access =  json.loads(response.text)
            self.token = access.get('access_token',None)
            self.auth_headers = {
            'Authorization': f'Bearer {self.token}'
            }  
            return access
        else:
            logger.error(f" rocket Unauthenticated: {response.text}")
        
        
    def list_periods(self,invoice_type="All"):
        
        response = requests.get(f'{self.base_url}/azure/usages/periods?invoice_type={invoice_type}', headers=self.auth_headers)
        return response.json()
     
    def list_usage(self,**params):
        urlparams = urllib.parse.urlencode(params)
        url = f"{self.base_url}/azure/usages?{urlparams}"
        print(urlparams)
        response = requests.get(url, headers=self.auth_headers)
        try:
            return response.json()
        except:
            return response.text

    def list_prev_usage(self,**params):


        urlparams = urllib.parse.urlencode(params)
        url = f"{self.base_url}/azure/previous?{urlparams}"
        response = requests.get(url, headers=self.auth_headers)
        try:
            return response.json()
        except:
            return response.text


    def list_subscriptions(self):
        response = requests.get('{self.base_url}/azure/subscription', headers=self.auth_headers)
        return response.json()
    def usages_per_resource(self,period,subscription_id,customer_id=None,resource_group=""):
        logger.info(f"fetching per resource costs of {subscription_id} in period {period} ")
        sub = next(sub for sub in settings.subscriptions if sub['subscription_id'] == subscription_id)
        
        # costs =  open_file('racket_response.json') # TODO TEST
        costs = open_file(f"./assets/racket_responses/{sub['env']}{period}.json")
        if costs:
            logger.info("rocket fetch skiped, costs already fetched")
            return costs
        print(f"rocket  `./assets/racket_responses/{sub['env']}{period}.json`  not found localy")
        # exit(0)
        
        self.authenticate()
        
        if customer_id is None:
            customer_id  = DEFACULT_CUSTOMER_ID
        filters  = {
            # "period":period,
            "period":period,
            # "period":"current",
            "invoice_type":"All",
            "view_type":"resource",
            "cost_type":"customer",
            "reseller_id":"",
            "customer_id": customer_id,
            "subscription_id":subscription_id,
            "resource_group":resource_group,
            "resource":""
        }
        
        costs = self.list_usage(**filters)
        save_obj(costs,f"./assets/racket_responses/{sub['env']}{period}.json")
        print(f"cost count {costs['total']}")
        exit(0)

        return costs

# https://rocket.uat.enube.me/api/v1/azure/usages?cost_type=customer&view_type=resource_group&reseller_id=&customer_id=3a7479db-d9ff-4e0b-9d0a-380a5e714f6f&subscription_id=b2fd9f8c-0ed5-4f6e-9c93-75ae90718afa&resource_group=&resource=&period=2022-04&invoice_type=All

# cost_type=customer
# view_type=resource
# customer_id=3a7479db-d9ff-4e0b-9d0a-380a5e714f6f
# subscription_id=b2fd9f8c-0ed5-4f6e-9c93-75ae90718afa
# period=2022-03
# invoice_type=All
# cost_type=customer
# view_type=resource
# reseller_id=
# customer_id=3a7479db-d9ff-4e0b-9d0a-380a5e714f6f
# subscription_id=7fefb755-c8be-4623-8ecd-b0d39089f788
# resource_group=
# resource=
# period=current
# invoice_type=All

# def list_all_usages():
#     view_types = [ 'reseller','account','service', 'categories', 'tags' , 'resource']
#     billings = ['All']
#     period = "2022-03"
#     invoice_types = ['Billing','OneTime' ,'RI']
#     for invoice_type in invoice_types:
#         for billing in billings:
#             for view_type in view_types:
#                 data = list_usage(token,period,invoice_type,view_type)
#                 if type(data) == dict:
#                     # print(data)
#                     save_json(data,f'rocket_reports/usage-{billing}-{invoice_type}-{view_type}-2022-02.json')
#                 else:
#                     print(data) 


'''
Azure tags
for r in db.resources.find({}):
    azurer = azure_manager.resources.get(r['name'])
    if azurer['tags'] != r['tags']:
        azure_manager.resource.update(r['tags'])


'''
