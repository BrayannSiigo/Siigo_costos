


# from email.mime import base
# import resource
from itertools import count
import resource
from urllib import response
import datetime
from azure.mgmt.subscription import SubscriptionClient
from azure.mgmt.resource import ResourceManagementClient
# from azure.identity import AzureCliCredential


# from pyrsistent import inc

# from httplib2 import Credentials

from dict_json import save_obj,open_file
# from progress_bar import print_progress_bar
from resourses_management.ids import RandomIdMaker
# from bson.objectid import ObjectId
from  resourses_management.tags_processor import TagsProcessor

class AzureFetcher():
    def __init__(self,id_factory,credentials):
        self.credentials = credentials 
        self.id_factory = id_factory
    def fetch(self,*args,**kwargs):
        pass
    def clean_values(self,object):
        cleanned_obj = {}
        for key,value in object.items():
            clean_f  = getattr(self,'clean_'+key,None)
            if clean_f:
                key,value  = clean_f(value)
            if value is not None:
                cleanned_obj[key] = value
        return cleanned_obj

class SubscriptionFetcher(AzureFetcher):
    def __init__(self,id_factory,credentials):
        super().__init__(id_factory,credentials)
        self.sub_client = SubscriptionClient(credentials)
    def fetch(self,include = None,exclude = None):
        # self.id_factory = RandomIdMaker()
        # id_length = self.ids_length['subscriptions']
        # print("include",include)
        # print("exclude",exclude)
        # print(list(self.sub_client.subscriptions.list() ))
        subscriptions = [self.clean_values({ **sub.as_dict(),**{'_id': self.id_factory.make_id()}}) for sub in self.sub_client.subscriptions.list() if include is  None or sub.subscription_id in include if  exclude is None or sub.subscription_id not in exclude  ]
        # print(subscriptions)
        # save_obj(subscriptions,'subscriptions.json')
        return subscriptions
        # c.resources_db.subscriptions.update_one({"subscription_id":"b2fd9f8c-0ed5-4f6e-9c93-75ae90718afa"},{"$set":{"stage":"QA"}})
    
    def clean_display_name(self,value):
        return "name",value
    def clean_id(self,value):
        return "id",None
class ResourceGroupsFetcher(AzureFetcher):
    def __init__(self,id_factory,credentials,tags_processor,tags_properties=None):
        AzureFetcher.__init__(self,id_factory,credentials)

        self.tags_processor =  tags_processor
        if tags_properties is None:
            tags_properties = ['owner','mail','tribu','description','creation_date','env','access_level','country','product']
        self.tags_properties = tags_properties
        self.properties = ['name','location','_id','id']
        self.rg_client = ResourceManagementClient(credentials,"")
    def fetch(self,subscription,map_info ={},include=None,exclude = []):
        
        # self.rg_client._config.subscription_id = subscription['az_id']
        resource_groups = []
        self.rg_client = ResourceManagementClient(self.credentials,subscription['az_id'])
        # print(list(self.rg_client.resource_groups.list()))
        rgs = (rg for rg in self.rg_client.resource_groups.list()  \
                if  include is  None or rg.name.lower() in include \
                if   exclude is None or rg.name.lower() not in exclude )
        
        
        # print(map_info)

        for rg in rgs:
            resource_group =  {k:v for k,v in rg.as_dict().items() if k in self.properties} 
            if not  resource_group['name']:
                print(f"resource_group dont have any name {resource_group}")
            # process tags
            print(resource_group['name'])
            if rg.tags:
                tags = self.tags_processor.process_tags( rg.tags )
                rg_properties  = {k:v for k,v in tags.items() if k in self.tags_properties} 
                extra_info =  {k:v for k,v in tags.items() if k not  in      self.tags_properties} 
                resource_group.update(rg_properties)
                if extra_info:
                    resource_group['extra_info']  = extra_info
            # predefinited info
            base_id = None
            if rg.name.lower() in map_info:
                # print("found info",rg.name)

                predefined_info = map_info[rg.name.lower()]
                base_id = predefined_info.pop("base_id") if "base_id" in predefined_info else None
                resource_group.update(predefined_info)
            # else:
                # print("not predefinited info",rg.name)
            # extra info
            # print(subscription)
            resource_group['base_id'],resource_group['short_id'] =  self.id_factory.make_id(base_id= base_id ,env=subscription['env'],respose_base_id = True)
            resource_group['subscription'] = subscription['internal_id']
            # print(resource_group)
            resource_group = self.clean_values(resource_group)
            # if 'extra_info' in resource_group:
            #     input(resource_group)
            resource_groups.append(resource_group)

        return resource_groups
    def clean_name(self,name):
        return "name", name.lower()
    def clean_owner(self,owner):
        # 1. TODO [ ] Buscar el Owner, Puede ser directamente en mongo, 
        #             o  se precarga la info de los usuarios y se busca de forma mas inteligente teniendo en cuenta, que
                    #   es un campo llenado por los propios usuarios
        # 2. TODO [ ] Retornar el email
        if owner:
            return "owner", owner
    def clean_id(self,vid):
        return "az_id",vid.lower()
    def clean_creation_date(self,str_date):
        # TODO return a date object 
        # str_date = resource_group.get('tags',{}).get('Date') # es un string
        def get_date():
            try:
                return datetime.datetime.strptime(str_date, "%d-%m-%Y")
            except:
                pass
            try:
                return datetime.datetime.strptime(str_date, "%Y-%m-%d")
            except:
                pass
            try:
                return datetime.datetime.strptime(str_date, "%Y-%d-%m")
            except:
                pass
            try:
                return datetime.datetime.strptime(str_date, "%d/%m/%Y")
            except:
                pass
            try:
                return datetime.datetime.strptime(str_date, "%Y/%m/%d")
            except:
                pass
            try:
                return datetime.datetime.strptime(str_date, "%Y/%d/%m")
            except:
                pass
        date = get_date()
        return "creation_date",date
class ResourceFetcher(AzureFetcher):
    def __init__(self,id_factory,credentials):
        super().__init__(id_factory,credentials)
        self.rg_client = ResourceManagementClient(credentials,"")
    def clean_type(self,value):
        # https://docs.microsoft.com/en-us/azure/governance/resource-graph/reference/supported-tables-resources
        return "type",value
    def clean_id(self,value):
        # https://docs.microsoft.com/en-us/azure/governance/resource-graph/reference/supported-tables-resources
        return "az_id",value.lower()
    def fetch(self, subscription, resource_groups=None ,include=None,exclude = []):
        def resources_by_rg(rg_name):
            # if rg_name == "rg-azsql-nubedbs-col-northcentral":
            #     print("founddddddd"*100)
            try:
                if self.rg_client.resource_groups.check_existence(rg_name):
                    resources = self.rg_client.resources.list_by_resource_group(rg_name)
                    # if rg_name == "rg-azsql-nubedbs-col-northcentral":
                    #     resources = list(resources)
                    #     print(f"len resources {len(resources)}")
                    #     print(f"resources {[r.name for r in resources]}")

                    return resources
                else:
                    print(f"rg {rg_name} has not found")
                    # TODO [] Desactivar el rg en el sistema
            except Exception as e:
                print(f'resources_by_rg: {e}')
            return [] 
            

        # TODO [ ] ignore resource types 
        self.rg_client._config.subscription_id = subscription['az_id']
        if resource_groups is None:
            resource_groups = (rg.as_dict() for rg in self.rg_client.resource_groups.list())
        all_resources = []
        for rg in resource_groups:
            # print(len(list(self.rg_client.resources.list_by_resource_group(rg['name']))))
            resources = (resource for resource in resources_by_rg(rg['name'])\
                        if include is None or resource.id.lower() in include \
                        if  exclude is None or resource.id.lower()  not in exclude \
                        if rg['name'])
            count = 0 

            for resource in resources:
                resource = resource.as_dict()
                count+=1
                if not  resource['name']:
                    print(f"resource dont have any name {resource}")

                resource['short_id'] =  self.id_factory.make_id(env=subscription['env'])
                resource['resource_group'] = rg['_id']
                if 'tags' in resource:
                    resource['extra_info'] = resource.pop('tags')
                resource = self.clean_values(resource)
                if 'az_id' not in resource:print(f"resource {resource['name']} doesnt have az_id")
                all_resources.append(resource)
            # print(f"found {count} resources  in {rg['name']}")
        return all_resources
    def clean_name(self,value):
        return "name",value.lower()
def test_subs():
    credentials = AzureCliCredential()
    id_factory = RandomIdMaker(length=4)
    azf_subs = SubscriptionFetcher(id_factory=id_factory,credentials=credentials)
    subs = azf_subs.fetch()#exclude=["9dc55c9d-84e5-442d-8951-92853102ae82"],include=['167fa7db-5dd5-4f30-9ce4-79c30cd3f293'])
        
    save_obj(subs,'subscriptions1.json')
def test_rgs():
    id_factory = RandomIdMaker(length=8)
    credentials = AzureCliCredential()
    azf_rg =  ResourceGroupsFetcher(id_factory,credentials,TagsProcessor(),['owner','description','creation_date','env','access_level','country'])
    sub  = { "internal_id": 'dEMz',"az_id" : 'b2fd9f8c-0ed5-4f6e-9c93-75ae90718afa',"env":'QA'}
    maps_regQa = {
        'rgqacolesiigo02':{
            "base_id":"Q0pSXlhv"
        },
        'rgqacolfunctions02':{
            'owner': "definido en el mappp, no se pone base id"
        },
        'cloud-shell-storage-southcentralus':{
            'base_id':"definidodesdemap",
            "location":'location predefinida'
        }
    }
    exclude = ['rgQAMEXFrontManager']
    resource_groups  = azf_rg.fetch(subscription=sub,exclude=exclude, map_info=maps_regQa)

    save_obj(resource_groups,'resoutce_groupsQA.json',default=str)

def test_resources():
    id_factory = RandomIdMaker(length=12)
    credentials = AzureCliCredential()
    azf_r =  ResourceFetcher(id_factory,credentials)
    sub  = { "internal_id": 'dEMz',"az_id" : 'b2fd9f8c-0ed5-4f6e-9c93-75ae90718afa',"env":'QA'}
    # maps_regQa = {
    #     'rgqacolesiigo02':{
    #         "base_id":"Q0pSXlhv"
    #     },
    #     'rgqacolfunctions02':{
    #         'owner': "definido en el mappp, no se pone base id"
    #     },
    #     'cloud-shell-storage-southcentralus':{
    #         'base_id':"definidodesdemap",
    #         "location":'location predefinida'
    #     }
    # }
    exclude = ['rgQAMEXFrontManager']
    resource_groups = open_file("resoutce_groupsQA.json")
    resources  = azf_r.fetch(subscription=sub,exclude=exclude, resource_groups=resource_groups)

    save_obj(resources,'resourcesQA.json',default=str)
# test_subs()
# test_rgs()
# test_resources()

# # azf.fetch_subscriptions()
# subs = [('cCVH', '9dc55c9d-84e5-442d-8951-92853102ae82','DEV'),
#  ('KGFH', '167fa7db-5dd5-4f30-9ce4-79c30cd3f293','DEVWCP'),
#  ('LGks', 'b2fd9f8c-0ed5-4f6e-9c93-75ae90718afa','QA'),
#  ('aDdQ', '7fefb755-c8be-4623-8ecd-b0d39089f788','PROD1'),
#  ('aWhi', '817e1f21-9d87-4bd6-bc04-cd55f28ae39b','PROD2'),
#  ('NS1U', '1c8012bc-73d9-4769-87ac-27a65791d611','CONT'),
# #  ('UDJM', '36108532-400c-4446-acd3-c8ebeda610ea','EP'),
#  ('bCxw', 'cd92b809-fa0a-46d9-90d7-fd06550e06da','CH'),
#  ('RCg2', 'd322b162-e996-4802-882c-4062b11e04b3','MX'),
#  ('bHRt', 'f13abb61-c114-4c30-ba00-1f4de2e33f57','EQ')]

# sub = {
#     'internal_id':'LGks', 
#     'az_id':'b2fd9f8c-0ed5-4f6e-9c93-75ae90718afa',
#     'env':'QA',
# }


# # resource_groups =  azf.fetch_resource_groups(sub )
# # save_obj(resource_groups,'resource_groups.json')
# resource_groups = open_file('resource_groups.json')
# resources =  azf.fetch_resources(resource_groups,sub)
# save_obj(resources,'resources.json')

#   TODO
# [ * ] TODO Crear IFetcher SubscriptionFetcher, RgFetcher(unico con RelatedIdMaker,RandomIdMaker), Resource Fetcher
# [* ] TODO Crear una funcion que arme los atributos finales de cada objeto\
# [ ] TODO usar yield para ir entregando los rg(Por los ids repetidos) 
# [ ] TODO Crear un Main que llame los fetch y guarde en la db
        # Fetch subscriptions
        # Fetch rg(internal_sub_id)
        #     - db.subs.findone(subid)
        # Fetch resources(internal_sub_id)
        #     - db.subs.findone(subid)
        #     - db.subs.find({'subid':subid})

        # Fetch rg(internal_sub_id,ids_map)
        #     - db.subs.findone(subid)

# [ ] TODO Crear un comanline que reciba parametros y llame al main
        #    Fetch subscriptions
        #    Fetch rg(internal_sub_id)
        #    Fetch resources(internal_sub_id)
        #    Fetch rg(internal_sub_id,ids_map)