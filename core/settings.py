import logging.config
import os
from pymongo import MongoClient
import pytz
from dict_json import open_file
logging.config.fileConfig( 'core/.logging.conf' , disable_existing_loggers=False )
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)

logger = logging.getLogger()
logger.debug("logger configured")

LOCAL_DB_COSTS = os.environ['LOCAL_DB_COSTS']
PROD_DB_COSTS = os.environ['PROD_DB_COSTS']
COSTS_COLLECTION = os.environ['COSTS_COLLECTION']
SUBSCRIPTIONS_COLLECTION = os.environ['SUBSCRIPTIONS_COLLECTION']
RESOURCE_GROUPS_COLLECTION = os.environ['RESOURCE_GROUPS_COLLECTION']
RESOURCES_COLLECTION = os.environ['RESOURCES_COLLECTION']
# UNMAPPED_COSTS_COLLECTION = os.environ['UNMAPPED_COSTS_COLLECTION']


LOCAL_MONGO_CONNECTION = os.environ['LOCAL_MONGO_CONNECTION']
PROD_MONGO_CONNECTION = os.environ['PROD_MONGO_CONNECTION']
SYNC_M_COLLECTION = os.environ['SYNC_M_COLLECTION']
localmongo = MongoClient(LOCAL_MONGO_CONNECTION)
remongo = MongoClient(PROD_MONGO_CONNECTION)
local_db= MongoClient(LOCAL_MONGO_CONNECTION)[LOCAL_DB_COSTS]
remote_db= MongoClient(PROD_MONGO_CONNECTION)[PROD_DB_COSTS]

subscriptions_collection = local_db[SUBSCRIPTIONS_COLLECTION]
resource_groups_collection = local_db[RESOURCE_GROUPS_COLLECTION]
resources_collection = local_db[RESOURCES_COLLECTION]
costs_collection = remote_db[COSTS_COLLECTION]
sync_management_collection = remote_db[SYNC_M_COLLECTION]



az_auth_method = os.environ.get('az_auth_method',"SDK")
az_credentials_file = os.environ.get('az_credentials_file',"core/.az_credentials.json")
az_manager_type = "SDK"

timezone = pytz.timezone(os.environ['timezone'])
fetch_frecuency =  int(os.environ.get('fetch_frecuency',2))


subscriptions = list(subscriptions_collection.find({}) )





def check_index_existence(db,collection_name,*fields ): 
    for _,value in db[collection_name].index_information().items():
        if len(value['key'])!= len(fields):
            continue
        not_match = False
        for vfield,direction in value['key']:
            if vfield not in fields:
                not_match = True
                break
        if not_match == False:
            return True
    return False


if not check_index_existence(local_db,RESOURCES_COLLECTION,"az_id"):
    local_db[RESOURCES_COLLECTION].create_index("az_id",unique=True)

if not check_index_existence(remote_db,RESOURCES_COLLECTION,"az_id"):
    remote_db[RESOURCES_COLLECTION].create_index("az_id",unique=True)


if not check_index_existence(local_db,RESOURCES_COLLECTION,"rocket_id"):
    local_db[RESOURCES_COLLECTION].create_index("rocket_id",unique=True,\
    partialFilterExpression={"rocket_id":{"$type":"string"}})

if not check_index_existence(remote_db,RESOURCES_COLLECTION,"rocket_id"):
    remote_db[RESOURCES_COLLECTION].create_index("rocket_id",unique=True,\
    partialFilterExpression={"rocket_id":{"$type":"string"}})

# if not check_index_existence(local_db,RESOURCE_GROUPS_COLLECTION,"az_id"):
#     local_db[RESOURCE_GROUPS_COLLECTION].create_index("az_id",unique=True)

# if not check_index_existence(remote_db,RESOURCE_GROUPS_COLLECTION,"az_id"):
#     remote_db[RESOURCE_GROUPS_COLLECTION].create_index("az_id",unique=True)

if not check_index_existence(local_db,SUBSCRIPTIONS_COLLECTION,"subscription_id"):
    local_db[SUBSCRIPTIONS_COLLECTION].create_index("subscription_id",unique=True)

if not check_index_existence(remote_db,SUBSCRIPTIONS_COLLECTION,"subscription_id"):
    remote_db[SUBSCRIPTIONS_COLLECTION].create_index("subscription_id",unique=True)

SERVICE_PRINCIPALS =    open_file("./core/.az_credentials.json")