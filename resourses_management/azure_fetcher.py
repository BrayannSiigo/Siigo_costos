import random
import time
from dict_json import save_obj,open_file
from core import settings

class AzureFetcherMain:
    def __init__(self,fetchers) -> None:
        self.subscriptions_fetcher = fetchers['subscriptions_fetcher']
        self.resource_groups_fetcher = fetchers['resource_groups_fetcher']
        self.resources_fetcher = fetchers['resources_fetcher'] 
        self.db =  settings.local_db
    def insert_many(self,resource_type,objects):
        # save_obj(objects,f"{resource_type}-{random.randint(100,10000)}")
        len_obj = len(objects)
        split = int(len_obj*2/12)
        print(f"len obj is {len_obj} split is {split}")
        if split<=0:
            return self._insert_many(resource_type,objects)
        index = 0
        index_rigth =  split
        while index_rigth<=len_obj:
            sub_objects  = objects[index:index_rigth]
            print(f"inserting [{index},{index_rigth-1}]")
            self._insert_many(resource_type,sub_objects)
            index  = index_rigth
            index_rigth+=split
            time.sleep(1.2)
    def _insert_many(self,resource_type,objects):

        count = 0 
        try:
            print(f"len  {resource_type} = {len(objects)}")
            if objects:
                r = self.db[resource_type].insert_many(objects,ordered=False)
                count = len(r.inserted_ids)
            else:
                settings.logger.info("There aren't new resources to be fetched")
        except Exception as e:
            print(e)
            # TODO part inserted count
        return count
        

    def fetch(self,resource_type, **kwargs):

        fetcher_function  = "fetch_"+ resource_type
        settings.logger.debug(f"fetching {resource_type} {kwargs}")
        if hasattr(self,fetcher_function):
            exclude =  None # TODO que hacer con exclude ??
                          # se pueden agregar todos los que ya estan en la base de datos
                          # pero si se quiere actualizar se puede usar  map_info, pasando el aid que tiene,
                          # y crear otra funcion para actualizar en fetcher donde solo retorne los que actualizo
                          
            fetcher = getattr(self,fetcher_function)
            results = fetcher(**kwargs)
            # results = list(map(lambda x:{'name':x['name'],'base_id':""},results))
            # save_obj(results,resource_type+str(random.randint(100,1000))+'.json',default=str)
            # response = len(results)
            response = self._insert_many(resource_type,results)
            response = self._insert_many(resource_type+'_tosync',results)
            settings.logger.info(f"inserted {response} {resource_type}")
            return response
            # save_obj(results,resource_type+str(random.randint(100,1000))+'.json',default=str)
        else:
            raise Exception(f"Resource type {resource_type} is not supported")
    def fetch_subscriptions(self,**kwargs):
        #  TODO exclude
        exclude = list(map(lambda x: x['subscription_id'], self.db.subscriptions.find({},{"_id":0,"subscription_id":1})))
        subs = self.subscriptions_fetcher.fetch(exclude=exclude, **kwargs)
        for sub in subs:
            sub['env'] = input(f"type subscription env {sub['name']}: ")
        return subs
    def fetch_resource_groups(self,subscription_id,exclude=None,**kwargs):
        # sub  = { "internal_id": 'dEMz',"az_id" : 'b2fd9f8c-0ed5-4f6e-9c93-75ae90718afa',"env":'QA'}
        
        sub = self._get_subscription(subscription_id)
        if exclude is None:
            exclude = list(map(lambda x: x['name'], self.db.resource_groups.find({},{"_id":0,"name":1})))
        # print("exclude: ",exclude)
        return  self.resource_groups_fetcher.fetch(subscription=sub,exclude=exclude, **kwargs)

    def fetch_resources(self,subscription_id,resource_groups = None,**kwargs):
        # sub  = { "internal_id": 'dEMz',"az_id" : 'b2fd9f8c-0ed5-4f6e-9c93-75ae90718afa',"env":'QA'}
        # return open_file("resources-8091.json")
        sub = self._get_subscription(subscription_id)
        rg_filter   = {"name":{"$in":resource_groups} } if resource_groups else {}
        rg_filter["subscription"] = sub["internal_id"]
        resource_groups =  list(self.db.resource_groups.find(rg_filter))

        exclude = list(map(lambda x: x['az_id'].lower(), self.db.resources.find({},{"_id":0,"az_id":1})))
        # exclude = []
        # print("exclude: ",exclude)
        # print(rg_filter)
        # print(resource_groups)
        return  self.resources_fetcher.fetch(subscription=sub,resource_groups=resource_groups, exclude=exclude, **kwargs)

    def _get_subscription(self,subscription_id):
        pipeline = [
            {
                '$project':{
                     'internal_id':'$_id', 
                     'az_id':'$subscription_id',
                     "env":"$env",
                     "_id":0
                     }
            },
            {
                "$match":{
                    "az_id":subscription_id
                    }
            }
        ]

        try:
            return next( self.db.subscriptions.aggregate(pipeline))
        except StopIteration:
            raise Exception(f"Subscription {subscription_id} does not exist")
# def test_rgs():

# def test_rgs():
#     id_factory = RandomIdMaker(length=8)
#     credentials = AzureCliCredential()
#     azf_rg =  ResourceGroupsFetcher(id_factory,credentials,TagsProcessor(),['owner','description','creation_date','env','access_level','country'])
#     sub  = { "internal_id": 'dEMz',"az_id" : 'b2fd9f8c-0ed5-4f6e-9c93-75ae90718afa',"env":'QA'}
#     maps_regQa = {
#         'rgqacolesiigo02':{
#             "base_id":"Q0pSXlhv"
#         },
#         'rgqacolfunctions02':{
#             'owner': "definido en el mappp, no se pone base id"
#         },
#         'cloud-shell-storage-southcentralus':{
#             'base_id':"definidodesdemap",
#             "location":'location predefinida'
#         }
#     }
#     exclude = ['rgQAMEXFrontManager']
#     resource_groups  = azf_rg.fetch(subscription=sub,exclude=exclude, map_info=maps_regQa)

#     save_obj(resource_groups,'resoutce_groupsQA.json',default=str)

'''
ids_ob = open_file("ids_maps_prod.json")
for rg,key in zip(c.resources_db1.resource_groups.find({}),ids_ob):
    print(ids_ob[key])
save_obj(ids_ob,"ids_maps_prod.json")

'''