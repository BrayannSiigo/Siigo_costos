
from ast import Return
from os import sync
from datetime import datetime
from pydoc import resolve
from typing import final
from dateutil.relativedelta import relativedelta

from dict_json import open_file,save_obj
from costs_management.gateways.rocket_api import RocketAPI
from core import settings
from costs_management.azure_id_generator import RocketAzureIdGenerator
from sync import start_sync,complete_sync

def fix_az_id(az_id):
    ''''Esta funcion es una solicion temporal para los sub recursos de las  bases de datos que salen en los costos'''
    special_ids = ['azsql-colombia-01',
    'azsql-colombia-02',
    'azsql-colombia-03',
    'azsql-colombia-04',
    'azsql-colombia-05',
    'azsql-colombia-06',
    'azsql-colombia-07',
    "azsqlmi-esiigo",
    "azsql-chile-01",
    "azsql-interno"]
    final_ids = {
        'azsql-colombia-01':'/subscriptions/7fefb755-c8be-4623-8ecd-b0d39089f788/resourcegroups/rg-azsql-nubedbs/providers/microsoft.sql/servers/azsql-colombia-01',
        'azsql-colombia-02':'/subscriptions/7fefb755-c8be-4623-8ecd-b0d39089f788/resourcegroups/rg-azsql-nubedbs/providers/microsoft.sql/servers/azsql-colombia-02',
        'azsql-colombia-03':'/subscriptions/7fefb755-c8be-4623-8ecd-b0d39089f788/resourcegroups/rg-azsql-nubedbs/providers/microsoft.sql/servers/azsql-colombia-03',
        'azsql-colombia-04':'/subscriptions/7fefb755-c8be-4623-8ecd-b0d39089f788/resourcegroups/rg-azsql-nubedbs/providers/microsoft.sql/servers/azsql-colombia-04',
        'azsql-colombia-05':'/subscriptions/7fefb755-c8be-4623-8ecd-b0d39089f788/resourcegroups/rg-azsql-nubedbs/providers/microsoft.sql/servers/azsql-colombia-05',
        'azsql-colombia-06':'/subscriptions/7fefb755-c8be-4623-8ecd-b0d39089f788/resourcegroups/rg-azsql-nubedbs-col-northcentral/providers/microsoft.sql/servers/azsql-colombia-06',
        'azsql-colombia-07':'/subscriptions/7fefb755-c8be-4623-8ecd-b0d39089f788/resourcegroups/rg-azsql-nubedbs-col-northcentral/providers/microsoft.sql/servers/azsql-colombia-07',
        "azsqlmi-esiigo":"/subscriptions/817e1f21-9d87-4bd6-bc04-cd55f28ae39b/resourcegroups/esiigo/providers/microsoft.sql/managedinstances/azsqlmi-esiigo",
        "azsql-chile-01":"/subscriptions/cd92b809-fa0a-46d9-90d7-fd06550e06da/resourcegroups/rg-azsql-nubechile/providers/microsoft.sql/servers/azsql-chile-01",
        "azsql-interno":"/subscriptions/7fefb755-c8be-4623-8ecd-b0d39089f788/resourcegroups/rg-azsql-nubedbs/providers/microsoft.sql/servers/azsql-interno"

    }
    for special_id in special_ids:
        if special_id in az_id:
            final_id = final_ids[special_id]
            settings.logger.info(f"Rocket AZ_ID {az_id} fixed as {final_id}")
            return final_id
    return az_id

class RocketCostsProcessor:
    def __init__(self,subscription_id):
        self.rocket =RocketAPI()
        self.subscription_id = subscription_id
        self.subscription = settings.subscriptions_collection.find_one({"subscription_id":subscription_id})

        if self.subscription is None:
            settings.logger.exception(f"Subscription {subscription_id} does not exist")
            raise Exception(f"Subscription {subscription_id} does not exist")
        print(self.subscription)
        self.id_generator = RocketAzureIdGenerator(self.subscription)

    def select_resource_id(self,cost_obj):
        resource = None
        # possible_id = None
        for possible_id in self.id_generator.resource_id_of_racket_cost(cost_obj):
            # print(possible_id)
            resource = settings.resources_collection.find_one({"az_id":possible_id})
            if resource:
                return resource['_id']
    def select_resource_idv1(self,cost_obj,azure_costs):
            found=False
            _id = None
            az_type  =None
            racket_id = self.generate_racket_id(cost_obj)
            try:
                for cost in azure_costs:
                    if racket_id == cost['rocket_id']:
                        # if found and az_type != cost['type']:
                        #     print("error, aparece dos veces",cost_obj['resource'])
                        # else:
                        #     az_type = cost['type']

                        # found = True
                        _id = cost["az_id"]

                        # return cost[4]
                        return fix_az_id(_id)
            except Exception as e:
                print(e)
                print(cost_obj)
                # print(cost)
                raise e
            if 'azure devops' in racket_id:
                return "/subscriptions/7fefb755-c8be-4623-8ecd-b0d39089f788/providers/microsoft.visualstudio/organizations/siigodevops"

    def generate_racket_id(self,cost_obj):
        return self.id_generator.generate_racket_id(cost_obj)
    def get_resource_id_fromdb(self,cost_obj):
        count = settings.resources_collection.count_documents({"name":cost_obj['resource'].lower()})
        # print(cost_obj)
        # print(count)
        if count == 1:
            resource =   settings.resources_collection.find_one({"name":cost_obj['resource'].lower()})
            # print("resource")
            # print(resource)
            return resource['az_id']
        # print(kwargs)
        # exit(0)
    def fetch_costs(self,month,year,azure_costs):
        # [] TODO validate if this costs are already in db 
        # rocket
        date = datetime(month=month,year=year,day=1)+ relativedelta(months=1)
        period = f"{year}-{'{:02d}'.format(month)}"
        rocket_period = f"{date.year}-{'{:02d}'.format(date.month)}"
        # print(period)
        costs = self.rocket.usages_per_resource(rocket_period,self.subscription_id)["usages"][0]["items"]
        # costs   = open_file(f"outputs/rocket/prod-{period}.json")["usages"][0]["items"]
        settings.logger.info(f"processing per resource costs of {period} ")
        
        statistics = {
            'resource_found': { 'count':0,'sum':0}, # se encuentra rg y recurso
            # 'rg_not_found':{ 'count':0,'sum':0}, # no existe el rg
            'resource_not_found': { 'count':0,'sum':0} # existe el rg pero no el recurso
        }
    
        processed_costs = []
        for cost_obj in costs:
            resource_name = cost_obj["resource_id"]
            resource_cost = cost_obj["total"]
            resource_id  = self.select_resource_idv1(cost_obj,azure_costs) or self.get_resource_id_fromdb(cost_obj)
            
            process_status = "resource_found" if resource_id else "resource_not_found"
            resource_id =   resource_id if  resource_id else self.generate_racket_id(cost_obj) 
            statistics[process_status]['count']+=1
            statistics[process_status]['sum']+=resource_cost
            if "resource_category" not in cost_obj:
                settings.logger.warning(f"without category {resource_name.lower()}")
            cost_record = {
                    "resource_name": resource_name.lower(),
                    "resource_id": resource_id,
                    "category":cost_obj.get("resource_category",""),
                    "cost":resource_cost,
                    "resource_group": cost_obj["parent_id"].lower(),
                    "month":month,
                    "year":year,
                    "provider":"rocket",
                    # "process_status":process_status,
                    "subscription":self.subscription['_id']
                }
            if process_status != "resource_found":
                cost_record['unmapped'] = True
            processed_costs.append(cost_record)
            process_status = None

        settings.logger.info(f"process completed successfully, statistics: {statistics} ")
        self.id_generator.close()
        if processed_costs:
            settings.logger.info(f"saving  costs in {settings.COSTS_COLLECTION} collection")
            sync_id = start_sync(settings.COSTS_COLLECTION,period=period,subscription=self.subscription_id,provider="rocket")
            response  = settings.costs_collection.insert_many(processed_costs)
            response = {'inserted_ids':len(response.inserted_ids),'acknowledged':response.acknowledged,'statistics':statistics}
            complete_sync(sync_id,response)
            # save_obj(processed_costs,f"foud_r_costs-{self.subscription['_id']}-{period}.json",default=str)
            return response['inserted_ids']
        # save_obj(list_not_foud_costs,f"test/not_foud-{period}json")

# test_racket_costs_processor()