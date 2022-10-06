import os 
from core.settings import logger
# from billing_fetch_core.billing_fetcherv1 import RocketCostsProcessor
from costs_management.processors.billing_fetcherv1 import RocketCostsProcessor 
from costs_management.processors.billing_fetcher_azure import AzureCostsProcessor 
from dict_json import open_file
from sync import are_costs_already_fetched
from core import settings
# def main1():
#     year = int( os.environ["year"])
#     first_month = int( os.environ["first_month"] )
#     last_month = int( os.environ["last_month"] )
#     subscriptions =  os.environ["subscriptions"]
#     if not subscriptions or not first_month or not last_month or not year:
#         responses  = {'error':'subscriptions,first_month,last_month,year are required'}
#     else:
#         subscriptions = filter(lambda x: x,subscriptions.split("/"))
#         responses = []
#         for subscription_id in subscriptions:
#             costs_processor = CostProcessor(subscription_id)
#             for month in range(first_month,last_month+1):
#                 responses.append({
#                     'year':year,
#                     'month':month,
#                     'subscription_id':subscription_id,
#                     'count':costs_processor.fetch_costs(month,year,)
#                     # 'count':costs_processor.fetch_costs(month,year)
#                 })
                
#     logger.info(responses)


def fetch_costs(month,year,provider,subscriptions):
    period = f"{year}-{'{:02d}'.format(month)}"
    
    responses = []
    # print(list(subscriptions))
    for subscription_id in subscriptions:
        # print(subscription_id)
        if not are_costs_already_fetched(year=year,month=month,subscription_id=subscription_id,provider=provider):
            sub = next(sub for sub in settings.subscriptions if sub['subscription_id'] == subscription_id)
            # print(sub)
            if provider == "rocket":
                CostProcessor = RocketCostsProcessor
                print(f"sub: {sub}")
                if sub['env'] == 'qa':
                    # azure_costs = open_file(f"./assets/azure_responses/{sub['env']}{period}.json") # TODO usar el azure_api.py
                    azure_costs = open_file("./assets/azure_responses/preprocessed/qa(2022, 1)_(2022, 8).json") # TODO usar el azure_api.py
                elif sub['env'] == 'prod':
                    # print("s"*100)
                    azure_costs = open_file("./assets/azure_responses/preprocessed/prod(2022, 1)_(2022, 8).json") # TODO usar el azure_api.py
                elif sub['env'] == 'nub':
                    azure_costs = open_file("./assets/azure_responses/preprocessed/nub(2022, 4)_(2022, 9).json") # TODO usar el azure_api.py
                    # azure_costs = open_file(f"./assets/azure_responses/{sub['env']}{period}.json") # TODO usar el azure_api.py
                elif sub['env'] == 'ch':
                    azure_costs = open_file("./assets/azure_responses/preprocessed/ch(2022, 4)_(2022, 9).json") # TODO usar el azure_api.py
                elif sub['env'] == 'mark':
                    azure_costs = open_file("./assets/azure_responses/preprocessed/mark(2022, 4)_(2022, 9).json") # TODO usar el azure_api.py
                else: 
                    print("Asdasdsad")  
                kwargs = {'month':month,"year":year,"azure_costs":azure_costs}
            else:
                CostProcessor =  AzureCostsProcessor
                kwargs = {'month':month,"year":year}
            # print(azure_costs)
            # print(provider)
            costs_processor = CostProcessor(subscription_id)
            responses.append({
                'year':year,
                'month':month,
                'subscription_id':subscription_id,
                'count':costs_processor.fetch_costs(**kwargs)
            })
        
    logger.info(responses)
    return responses

# test_generate_excel_report()

# run_racket_fetch()
# run_billing_fetcher()
# azure_costs = open_file("./azure_costs_0522.json")

# test 

# year = 2022
# for month in range(1,6):
#     period = f"{year}-{'{:02d}'.format(month)}"
#     # azure_costs = open_file(f"./assets/azure_responses/qa{period}.json")
#     # print(type(azure_costs))
#     print("hola")
#     fetch_costs(month,year)