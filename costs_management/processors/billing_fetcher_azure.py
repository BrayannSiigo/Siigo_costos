
from datetime import datetime
from dateutil.relativedelta import relativedelta


from os import sync
from dict_json import open_file,save_obj
# from billing_fetch_core.rocket_api import RocketAPI
from costs_management.gateways.azure_api import APIBillingManager
from core import settings

# from billing_fetch_core.azure_id_generator import RocketAzureIdGenerator
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
    "azsql-chile-01"]
    final_ids = {
        'azsql-colombia-01':'/subscriptions/7fefb755-c8be-4623-8ecd-b0d39089f788/resourcegroups/rg-azsql-nubedbs/providers/microsoft.sql/servers/azsql-colombia-01',
        'azsql-colombia-02':'/subscriptions/7fefb755-c8be-4623-8ecd-b0d39089f788/resourcegroups/rg-azsql-nubedbs/providers/microsoft.sql/servers/azsql-colombia-02',
        'azsql-colombia-03':'/subscriptions/7fefb755-c8be-4623-8ecd-b0d39089f788/resourcegroups/rg-azsql-nubedbs/providers/microsoft.sql/servers/azsql-colombia-03',
        'azsql-colombia-04':'/subscriptions/7fefb755-c8be-4623-8ecd-b0d39089f788/resourcegroups/rg-azsql-nubedbs/providers/microsoft.sql/servers/azsql-colombia-04',
        'azsql-colombia-05':'/subscriptions/7fefb755-c8be-4623-8ecd-b0d39089f788/resourcegroups/rg-azsql-nubedbs/providers/microsoft.sql/servers/azsql-colombia-05',
        'azsql-colombia-06':'/subscriptions/7fefb755-c8be-4623-8ecd-b0d39089f788/resourcegroups/rg-azsql-nubedbs-col-northcentral/providers/microsoft.sql/servers/azsql-colombia-06',
        'azsql-colombia-07':'/subscriptions/7fefb755-c8be-4623-8ecd-b0d39089f788/resourcegroups/rg-azsql-nubedbs-col-northcentral/providers/microsoft.sql/servers/azsql-colombia-07',
        "azsqlmi-esiigo":"/subscriptions/817e1f21-9d87-4bd6-bc04-cd55f28ae39b/resourcegroups/esiigo/providers/microsoft.sql/managedinstances/azsqlmi-esiigo",
        "azsql-chile-01":"/subscriptions/cd92b809-fa0a-46d9-90d7-fd06550e06da/resourcegroups/rg-azsql-nubechile/providers/microsoft.sql/servers/azsql-chile-01"
        

    }
    for special_id in special_ids:
        if special_id in az_id:
            final_id = final_ids[special_id]
            settings.logger.info(f"Rocket AZ_ID {az_id} fixed as {final_id}")
            return final_id
    return az_id


class AzureCostsProcessor:
    def __init__(self,subscription_id):
        self.subscription_id = subscription_id
        self.subscription = settings.subscriptions_collection.find_one({"subscription_id":subscription_id})

        if self.subscription is None:
            settings.logger.exception(f"Subscription {subscription_id} does not exist")
            raise Exception(f"Subscription {subscription_id} does not exist")
        self.azure =APIBillingManager(self.subscription)
        

     
    def group_costs(self,costs):
        '''this function groups costs of azure response, by resource_id'''
        ''' costs might be  sorted by resource_id'''
        costs.sort(key=lambda x:x[4])
        record = costs[0]
        len_records   = len(costs)
        for index in range(1,len_records):
            record_i = costs[index]
            record_i[4] = record_i[4].replace("/slots/staging","")
            if record_i[4] == record[0]:
                record[0]+=record_i[0]
            else:
                yield record
                if len_records > index+1:
                    record = costs[index+1]
        
    def fetch_costs(self,month,year):
        # date1 = datetime(year=year,month=month,day=1)
        # date2 = date1 + relativedelta(months=1) - relativedelta(days=1)
        # date2 = date1  + relativedelta(months=1) - relativedelta(seconds=1)
        # timePeriod = {
        #             "from": date1.isoformat()+".000Z",
        #             "to":date2.isoformat()+".000Z"

        # }

        period = f"{year}-{'{:02d}'.format(month)}"
        costs =  self.azure.billing_by_resource(period=(month,year))
        input(type(costs))
        settings.logger.info(f"processing per resource costs of {(month,year)} ")
        
        statistics = {
            'resource_found': { 'count':0,'sum':0}, # se encuentra rg y recurso
            # 'rg_not_found':{ 'count':0,'sum':0}, # no existe el rg
            'resource_not_found': { 'count':0,'sum':0} # existe el rg pero no el recurso
        }
        processed_costs = []    
        groups  = self.group_costs(costs)
        groups  = list(groups)
        print(len(groups))
        for record in groups:
            resource_cost,rg,azure_type,location,az_id,service_name,_,_,tags,_ = record
            rg,azure_type,location,az_id,service_name = map(lambda x:x.lower(),(rg,azure_type,location,az_id,service_name))
            az_id = fix_az_id(az_id)
            resource_name =  az_id.rsplit("/",1)[1]


        
            process_status = "resource_found"
            statistics[process_status]['count']+=1
            statistics[process_status]['sum']+=resource_cost
            cost_record = {
                    "resource_name": resource_name.lower(),
                    "resource_id": az_id,
                    "cost":resource_cost,
                    "resource_group": rg,
                    "month":month,
                    "year":year,
                    "provider":"azure",
                    "subscription":self.subscription['_id']
                }
            processed_costs.append(cost_record)

        settings.logger.info(f"process completed successfully, statistics: {statistics} ")
        if processed_costs:
            settings.logger.info(f"saving  costs in {settings.COSTS_COLLECTION} collection")
            sync_id = start_sync(settings.COSTS_COLLECTION,period=period,subscription=self.subscription_id,provider="azure")
            response  = settings.costs_collection.insert_many(processed_costs)
            response = {'inserted_ids':len(response.inserted_ids),'acknowledged':response.acknowledged,'statistics':statistics}
            complete_sync(sync_id,response)
            # save_obj(processed_costs,f"foud_r_costs-azure-{self.subscription['_id']}-{period}.json",default=str)
            return response['inserted_ids']

# test_racket_costs_processor()