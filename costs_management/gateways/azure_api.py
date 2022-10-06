
from datetime import datetime
from operator import sub
from pydoc import apropos
from dateutil.relativedelta import relativedelta

import requests 
import os
import json
import logging
from dict_json import save_obj,open_file
from core import settings

# TODO [ ] Class service principal auth
# TODO [ ] Selector de service principal dependiendo de la sub
# TODO [ ] Cargar los azure costs desde RocketCostsProcessor


# AZURE_TENANT_ID = os.environ['AZURE_TENANT_ID']
# AZURE_APP_ID = os.environ['AZURE_APP_ID']
# AZURE_PASSWORD = os.environ['AZURE_PASSWORD']
# AZURE_ACCESS_TOKEN = os.environ['AZURE_TOKEN']
AZURE_ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IjJaUXBKM1VwYmpBWVhZR2FYRUpsOGxWMFRPSSIsImtpZCI6IjJaUXBKM1VwYmpBWVhZR2FYRUpsOGxWMFRPSSJ9.eyJhdWQiOiJodHRwczovL21hbmFnZW1lbnQuY29yZS53aW5kb3dzLm5ldCIsImlzcyI6Imh0dHBzOi8vc3RzLndpbmRvd3MubmV0LzNhNzQ3OWRiLWQ5ZmYtNGUwYi05ZDBhLTM4MGE1ZTcxNGY2Zi8iLCJpYXQiOjE2NjM2ODcxNTcsIm5iZiI6MTY2MzY4NzE1NywiZXhwIjoxNjYzNjkyODEwLCJhY3IiOiIxIiwiYWlvIjoiQVRRQXkvOFRBQUFBV1YwaGg3OGtLemtnby9ZTHcxYWNhQmttR0luQ2RpakRuMXFXNW9VOEtVWU5DeS93UHZ1VElVWHNMYTFkbUZPTiIsImFtciI6WyJwd2QiXSwiYXBwaWQiOiIxOGZiY2ExNi0yMjI0LTQ1ZjYtODViMC1mN2JmMmIzOWIzZjMiLCJhcHBpZGFjciI6IjAiLCJmYW1pbHlfbmFtZSI6IlJhbWlyZXogQ2hhdmV6IiwiZ2l2ZW5fbmFtZSI6IlNhbnRpYWdvIEFuZHJlcyIsImdyb3VwcyI6WyJlYjM2YTJlYS0xNjg0LTQwMTQtYjE1MS1jOWM5MzExM2I3MTQiLCJkMDQ2MjlhYi0yZGNlLTRjY2QtYmFkOS0yMGFmYWM0MGQ0YjkiLCJmYWFlM2I1NS1jOGFiLTQ2YTMtOGNlYi1hYTlmMTY4ODIxNmUiLCIxNWRmZGQ5Yi1hZTA2LTQxYmMtODI3ZC01ODNkNDdjZTNjY2EiLCIwMzU3YmJhYS00N2YwLTQxMmEtYjAzNi0zYTk4YjhlZTNhM2QiLCI3NWZhYTJjNC01MGY0LTQ2Y2EtOWJjMy1lNDE2NTc5YzljYjUiLCJjNjA3NWU1Zi0yMWQ3LTRlN2UtYmRlZC1iNjkxOTVhZmI0MWIiLCI0NDA4MjNmOS1jN2NjLTRiNzMtYTVhNi05YTZjMDllN2E5NWQiLCJiNTg3NDM1Yi1mMTg2LTQ3ZWMtOTBjMC1iYTNkNWZiMjYyOGIiLCI4M2Q5OGUyMy1jZWRkLTQ1NjUtOTFiMy03YWVkZjNkNmVlYjciLCI3MGQyYmQyNy0zZGIwLTRiNDUtYjFkMC1iMTg1NjEwZDgzYjkiXSwiaXBhZGRyIjoiMTg2LjE1NC40Ni43IiwibmFtZSI6IlNhbnRpYWdvIEFuZHJlcyBSYW1pcmV6IENoYXZleiIsIm9pZCI6IjI5ODA3NTZhLThkZjQtNDljNS05NWQ3LTM4YjVhMjRlYjc3OSIsIm9ucHJlbV9zaWQiOiJTLTEtNS0yMS0yNzc4MDYxMzMwLTE0NDg5MDcxNjktMzA5MjQ0NDkzOC0xNzI5MSIsInB1aWQiOiIxMDAzMjAwMUIzQzNDQzI0IiwicmgiOiIwLkFUUUEyM2wwT3ZfWkMwNmRDamdLWG5GUGIwWklmM2tBdXRkUHVrUGF3ZmoyTUJNMEFHOC4iLCJzY3AiOiJ1c2VyX2ltcGVyc29uYXRpb24iLCJzdWIiOiJYWmJ2WlFFdzBKUjBFMDE3N1pIOE1aSVRhemkyYXVDUXQ2UF9acDV3YXprIiwidGlkIjoiM2E3NDc5ZGItZDlmZi00ZTBiLTlkMGEtMzgwYTVlNzE0ZjZmIiwidW5pcXVlX25hbWUiOiJyYW1pODAyMjg4QHNpaWdvLmNvbSIsInVwbiI6InJhbWk4MDIyODhAc2lpZ28uY29tIiwidXRpIjoiUms3V1QtaUlIazY0c2JSRWt1VWVBQSIsInZlciI6IjEuMCIsIndpZHMiOlsiYjc5ZmJmNGQtM2VmOS00Njg5LTgxNDMtNzZiMTk0ZTg1NTA5Il0sInhtc190Y2R0IjoxNDU3NDY5OTA2fQ.QWVngL9ixS9lfDdMn6rUPPupH4t-zPOii8b7OIhmJz-UKRGL6hhhD6XMe0Hj-T0q4mHNmBmj3ZDg9FNhG82MqgvWqbpR0NU94pbpopCclgO0XnC66KsV0w7_EAnxmQbhvQuZHEy2JiX2hTEB0OEuOszPYEh6y4sZYYFOUVw1AdY-pbWsCy4J8YeVDBFjWSV9brMbV8aaInN5grzV9yhWJEySLt_4J5vtWYSP8ty-6bFLeq5cwrJNaBurrx9FfBdeeEDz46brE0wVDF6f14aySsh0FGrJEngabTRmc0s1xx7Fib3Qouw6iG35cxCTF8cOkp1SNiFX98tM-HZ3T2cf-w"

class APIUnauthorizedException(Exception):
    pass

class APIAzureManager:
    def __init__(self,subscription):
        self.subscription = subscription
        self.base_url  = ""
        self.authenticate()


    def authenticate(self):
        # global AZURE_TENANT_ID
        # global AZURE_APP_ID
        # global AZURE_PASSWORD
        AZURE_TENANT_ID= '3a7479db-d9ff-4e0b-9d0a-380a5e714f6f'
        my_headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        url  = f"https://login.microsoftonline.com/{AZURE_TENANT_ID}/oauth2/v2.0/token"
        # print(settings.SERVICE_PRINCIPALS)
        # print(self.subscription)
        service_principal = settings.SERVICE_PRINCIPALS[self.subscription['subscription_id']]
        data = {
            "grant_type":"client_credentials",
            "client_id":service_principal['client_id'],
            "client_secret":service_principal['client_secret'],
            # "resource": "https://management.azure.com/",
            "scope": "https://management.azure.com/.default"
            # "scope": "Microsoft.CostManagement/Query/.default"
        }
        response = requests.post(url, headers=my_headers,data=data)
        print("------")
        # print(response)
        # print(response.text)
        # open("errorresponse.html","a").write(response.text)
        print("------")
        response = json.loads(response.text)
        if 'error' in response:
            raise APIUnauthorizedException('Invalid credentials')
        self.access_token = response['access_token']
        print(self.access_token)
        self.expires_on= response['expires_in']

class APIBillingManager(APIAzureManager):
    def __init__(self,subscription):
        APIAzureManager.__init__(self,subscription)
        self.subscription = subscription 
        self.base_url  = ""
        # self.access_token = AZURE_ACCESS_TOKEN
        # self.authenticate()
   
    def billing_by_resource(self,timePeriod=None,period=None):
        if timePeriod:
            filename = f"./assets/azure_responses/{self.subscription['env']}{timePeriod['from']}_{timePeriod['to']}.json"
        else:
            month,year = period
            filename=f"./assets/azure_responses/{self.subscription['env']}{month}-{year}.json"
            # costs =  open_file(f"./assets/azure_responses/{self.subscription['env']}{timePeriod['from']}_{timePeriod['to']}.json")
        settings.logger.info(filename)
        costs =  open_file(filename)
        if costs:
            settings.logger.info("azure fetch skiped, costs already fetched")
            return costs
    # def vpass():
        my_headers = {"Authorization" : f'Bearer {self.access_token}'}
        body = {
                "type": "Usage",
                "type": "ActualCost",
                # "timeframe": "TheLastMonth",
                "timeframe": "Custom",
                "timePeriod":timePeriod,
                # "timePeriod": {
                #     "from": "2022-01-01T00:00:00",
                #     "to": "2022-01-31T23:59:59"
                #     # "to": "2022-02-28T23:59:59"
                #     # "to": "2022-03-31T23:59:59"
                #     # "to": "2022-04-30T23:59:59"
                #     # "to": "2022-05-31T23:59:59"
                #     # "to": "2022-06-30T23:59:59"


                # },
                "dataset": {
                    "granularity": "None",
                    # "granularity": "Accumulated",
                    "aggregation": {
                    "totalCost": {
                        "name": "PreTaxCost",
                        "function": "Sum"
                    }
                    },
                    "include": [
                        "Tags"
                    ],
                    # "filter": {
                    #     "dimensions": {
                    #         "name": "ResourceGroupName",
                    #         "operator": "In",
                    #         "values": [
                    #         ]
                    #     }
                    # },
                    "grouping": [
                    # {
                    #     "type": "Dimension",
                    #     "name": "ResourceName"
                    # },
                    {
                        "type": "Dimension",
                        "name": "ResourceGroup"
                    },
                    {
                        "type": "Dimension",
                        "name": "ResourceType"
                    },
                    {
                        "type": "Dimension",
                        "name": "ResourceLocation"
                    },
                    {
                        "type": "Dimension",
                        "name": "ResourceId"
                    },
                    {
                        "name": "ServiceName",
                        "type": "Dimension"
                    },
                    {
                        "name": "MeterSubCategory",
                        "type": "Dimension"
                    },
                    {
                        "name": "Meter",
                        "type": "Dimension"
                    }
                    ],
                "sorting": [
                        {
                            "direction": "descending",
                            "name": "totalCost"
                        }
                    ]
                }
              }
        nextLink = f"https://management.azure.com/subscriptions/{self.subscription['subscription_id']}/providers/Microsoft.CostManagement/query?api-version=2021-10-01"
        print(nextLink)
        first = True
        while nextLink:
            response = requests.post(nextLink,headers=my_headers, json = body)
            # print(response.text)
            response = json.loads(response.text)
            # print(response)
            if first:
                columns = response['properties']['columns']
                yield columns
                first=False
            nextLink = response['properties']['nextLink']
            rows = response['properties']['rows']
            yield rows

    
# subscription_id = os.environ['AZURE_SUBSCRIPTION_ID']
# subscription_id = "b2fd9f8c-0ed5-4f6e-9c93-75ae90718afa"d
def generate_time_period(year,month,full_month=True,days=-1):
    date1 = datetime(year=year,month=month,day=1)
    # date2 = date1 + relativedelta(months=1) - relativedelta(days=1)
    if full_month:
        args = {'months':1}
    else:
        args = {'days':days}
    date2 = date1  + relativedelta(**args) - relativedelta(seconds=1)
    timePeriod = {
                "from": date1.isoformat()+".000Z",
                "to":date2.isoformat()+".000Z"

    }
    return timePeriod
def generate(subscription_id,year,month,full_month=True,days=-1):

    
    # print(settings.subscriptions)
    sub = next(sub for sub in settings.subscriptions if sub['subscription_id'] == subscription_id)

    # endPeriods=["2022-01-15T23:59:59.000Z",
    # "2022-02-14T23:59:59.000Z",
    # "2022-03-15T23:59:59.000Z",
    # "2022-04-15T23:59:59.000Z",
    # "2022-05-15T23:59:59.000Z",
    # "2022-06-15T23:59:59.000Z",
    # "2022-07-15T23:59:59.000Z"
    # ]
    manager = APIBillingManager(sub)
    all_costs = []
    if full_month:
        filename = f"./assets/azure_responses/{sub['env']}{month}-{year}.json"
    else:
        filename = f"./assets/azure_responses/{sub['env']}{timePeriod['from']}_{timePeriod['to']}.json"
    costs =  open_file(filename)
    if costs: return costs
    timePeriod = generate_time_period(year,month,full_month=full_month,days=days)
    # timePeriod = {
    #                 "from": f"2022-0{period}-01T00:00:00.000Z",
    #                 "to":endPeriods[period-1]

    # }

    print("fetching ",timePeriod)
    first = True
    for rows in manager.billing_by_resource(timePeriod):
        if first:
            print(rows)
            first = False
            continue
        print("\rworking\r")
        all_costs+=rows
    print(f"{timePeriod} total cost:{sum(c[0] for c in all_costs)}")
    
    print(f"saving to {filename}")
    save_obj(all_costs,filename)
    return all_costs
def run():

    # subscription_id =  "7fefb755-c8be-4623-8ecd-b0d39089f788"
    subscription_id =  "b2fd9f8c-0ed5-4f6e-9c93-75ae90718afa"
    # subscription_id =  "817e1f21-9d87-4bd6-bc04-cd55f28ae39b"   
    # subscription_id="cd92b809-fa0a-46d9-90d7-fd06550e06da"
    # subscription_id="8cd001c1-67fd-4553-b7a9-a25d59208462"
    # subscription_id="f13abb61-c114-4c30-ba00-1f4de2e33f57"

    for month in range(9,10):
        generate(subscription_id,2022,month,full_month=True)
run()
    # save_obj(all_costs,f"2022-0{period}.json")
# manager.authenticate()
# x = manager.all_resource_groups()[-1]
# print(x)

# print(manager.delete("testj7"))

