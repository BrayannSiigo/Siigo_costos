from costs_management.processors.billing_fetcher import RocketCostsProcessor
from dict_json import open_file
def test_racket_costs_processor(year,first_month,last_month,subscription_id):
    azure_costs = open_file("./azure_costs_0522.json")
    costs_processor = RocketCostsProcessor(subscription_id)
    for month in range(first_month,last_month+1):
        costs_processor.fetch_costs(month,year,)
# test_racket_costs_processor()   