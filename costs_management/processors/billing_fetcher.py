
from os import sync
from dict_json import open_file,save_obj
from costs_management.gateways.rocket_api import RocketAPI
from core import settings
from costs_management.azure_id_generator import RocketAzureIdGenerator
from sync import start_sync,complete_sync
class RocketCostsProcessor:
    def __init__(self,subscription_id):
        self.rocket =RocketAPI()
        self.subscription_id = subscription_id
        self.subscription = settings.subscriptions_collection.find_one({"subscription_id":subscription_id})

        if self.subscription is None:
            settings.logger.exception(f"Subscription {subscription_id} does not exist")
            raise Exception(f"Subscription {subscription_id} does not exist")
        print(self.subscription)
        self.id_generator = RocketAzureIdGenerator(subscription_id)

    def select_resource_id(self,cost_obj):
        resource = None
        # possible_id = None
        for possible_id in self.id_generator.resource_id_of_racket_cost(cost_obj):
            # print(possible_id)
            resource = settings.resources_collection.find_one({"az_id":possible_id})
            if resource:
                return resource['_id']
    def select_resource_idV1(self,cost_obj):
        category = cost_obj.get("resource_category",None)
        resource_group = cost_obj['parent_id']
        resource_name  = cost_obj['resource_id']
        if category:
            resource = settings.local_db.find_one({"resource_group":resource_group,"name":resource_name,"service_name":category}) or {}
            return resource.get("_id",None)
        
    def generate_racket_id(self,cost_obj):
        return self.id_generator.generate_racket_id(cost_obj)
    def fetch_costs(self,month,year):
        # [] TODO validate if this costs are already in db 
        period = f"{year}-{'{:02d}'.format(month)}"
        print(period)
        costs = self.rocket.usages_per_resource(period,self.subscription_id)["usages"][0]["items"]
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
            resource_id  = self.select_resource_idV1(cost_obj)
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
            sync_id = start_sync(settings.COSTS_COLLECTION,period=period,subscription=self.subscription_id)
            response  = settings.costs_collection.insert_many(processed_costs)
            response = {'inserted_ids':len(response.inserted_ids),'acknowledged':response.acknowledged,'statistics':statistics}
            complete_sync(sync_id,response)
            # save_obj(processed_costs,f"foud_r_costs-{self.subscription['_id']}-{period}.json",default=str)
            return len(response['inserted_ids'])
        # save_obj(list_not_foud_costs,f"test/not_foud-{period}json")

# test_racket_costs_processor()