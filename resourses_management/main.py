import os
from core.settings import logger
from resourses_management.factories import AzureFetcherFactory
# subscriptions="b2fd9f8c-0ed5-4f6e-9c93-75ae90718afa/7fefb755-c8be-4623-8ecd-b0d39089f788"
from core import settings
import sync
    


def fetch(subscriptions) -> None:
    sync_id = sync.start_local_fetch('resources')

    # subscriptions= settings.subscriptions_collection.find({'state':'Enabled'},{'subscription_id':1,'_id':0}) # os.environ['subscriptions'] #"b2fd9f8c-0ed5-4f6e-9c93-75ae90718afa/7fefb755-c8be-4623-8ecd-b0d39089f788"
    # subscriptions = (subscription['subscription_id'] for subscription in subscriptions )
    # subscriptions  = ["7fefb755-c8be-4623-8ecd-b0d39089f788","817e1f21-9d87-4bd6-bc04-cd55f28ae39b"]
    # print(subscriptions)
    # subscriptions=subscriptions.split("/")
    
    # subscriptions = filter(lambda x: x,subscriptions)
    
    fetcher =AzureFetcherFactory().build()
    # fetcher.fetch("subscriptions")
    results = {}
    for sub in subscriptions:   
        print(f"fetching resources of subscription {sub}")
        results[sub] = {
            "resource_groups":fetcher.fetch("resource_groups",subscription_id=sub),
            "resources":fetcher.fetch("resources",subscription_id=sub)
        }
    logger.info(f"fetching results {results}")
    sync.complete_local_fetch(sync_id,results=results)

    
# main()