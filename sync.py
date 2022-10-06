import pytz
from datetime import datetime
from core import settings 
SYNC_START = 'start'
SYNC_UPLOADED = 'uploaded'
SYNC_LOCAL_CLEANED = 'clean'
SYNC_COMPLETE = 'complete'
def get_timestamp():
    return datetime.now(tz= settings.timezone)

def sync_collection(collection_name):
    settings.logger.info(f'sync {collection_name} collection is starting')

    sync_id = settings.sync_management_collection.insert_one({'collection':collection_name,'status':SYNC_START,'start_timestamp':get_timestamp()}).inserted_id
    resources = list(settings.local_db[collection_name+'_tosync'].find({}))
    inserted_ids = []
    if resources:
        inserted_ids = settings.remote_db[collection_name].insert_many(resources,ordered=False).inserted_ids
    settings.sync_management_collection.update_one({"_id":sync_id},{'$set':{'status':SYNC_UPLOADED,'end_timestamp':get_timestamp(),'sync_len':len(inserted_ids)}})
    deleted_count  = settings.local_db[collection_name+'_tosync'].delete_many({}).deleted_count
    settings.sync_management_collection.update_one({"_id":sync_id},{'$set':{'status':SYNC_COMPLETE,'clean_timestamp':get_timestamp(),'clean_len':deleted_count}})
    settings.logger.info(f'sync {collection_name} collection has been done')

def sync_resource_groups():
    return sync_collection(settings.RESOURCE_GROUPS_COLLECTION)
def sync_resources():
    return sync_collection(settings.RESOURCES_COLLECTION)
def are_costs_already_fetched(month,year,subscription_id,provider):
    period = f"{year}-{'{:02d}'.format(month)}"
    sync_record = settings.sync_management_collection.find_one({'collection':settings.COSTS_COLLECTION,'period':period,'subscription':subscription_id,"provider":provider})
    if sync_record:
        if sync_record['status'] != SYNC_COMPLETE:
            settings.logger.warn(f'incomplete sync found for {period}  period {subscription_id} subscription_id')
            subscription  = settings.subscriptions_collection.find_one({'subscription_id':subscription_id})
            deleted_count =settings.costs_collection.delete_many({'month':month,'year':year,'subscription':subscription['_id']}).deleted_count
            settings.logger.warn(f'deleting all records({deleted_count}) for {period} period {subscription_id} subscription_id has been deleted ')
            # input('press enter []')
            settings.sync_management_collection.delete_one({'collection':settings.COSTS_COLLECTION,'period':period,'subscription':subscription_id})
            return False
        return True

    return False
def are_costs_already_fetched1(month,year,subscription_id,provider):
    r = are_costs_already_fetched(month,year,subscription_id,provider)
    # print("*"*10)
    # print(subscription_id)
    # print(r)
    # print("*"*10)
    # return True
def start_sync(collection,**kwargs):
    sync_id = settings.sync_management_collection.insert_one({'collection':collection,'status':SYNC_START,'start_timestamp':get_timestamp(),**kwargs}).inserted_id
    return sync_id
def complete_sync(sync_id,sync_data,**kwargs):
    settings.sync_management_collection.update_one({"_id":sync_id},{'$set':{'status':SYNC_COMPLETE,'end_timestamp':get_timestamp(),'sync_data':sync_data,**kwargs}})


def start_local_fetch(collection,**kwargs):
    sync_id = settings.sync_management_collection.insert_one({'collection':collection,'local':True,'status':SYNC_START,'start_timestamp':get_timestamp(),**kwargs}).inserted_id
    return sync_id
def complete_local_fetch(sync_id,**kwargs):
    settings.sync_management_collection.update_one({"_id":sync_id},{'$set':{'status':SYNC_COMPLETE,'end_timestamp':get_timestamp(),**kwargs}})
def query_resent_sync_record(collection,date_offset,**filters):
    limit_date =  get_timestamp() - date_offset
    return settings.sync_management_collection.find_one({'collection':collection,'start_timestamp':{'$gte':limit_date},**filters})

    

