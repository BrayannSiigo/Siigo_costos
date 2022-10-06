
from xml import dom
from dict_json import open_file,save_obj
from core import settings
import pymongo
from  resourses_management.tags_processor import TagsProcessor


# def generate_periods(first_period,last_period):
#     last_month=-1
#     for year in range(first_period[0],last_period[0]+1):
#         first_month = 1 if last_month == 12 else first_period[1]

#         last_month = last_period[1] if year == last_period[0] else 12
#         for month in range(first_month,last_month+1):
#             period = f"{year}-{'{:02d}'.format(month)}"
#             yield period

class ResourcesInCostsProcessor:
    '''Esta clase procesa la informacion de los recursos que se 
    encuentra en la respuesta de azure  cuando se consulta el uso de cada recurso
    '''
    def __init__(self,id_factory) -> None:
        self.tagsprocessor = TagsProcessor()
        self.rg_tags_properties = ['owner','mail','tribu','creation_date','env','access_level']
        self.r_tags_properties = ['owner','mail','tribu','description','creation_date','env','access_level']
        self.id_factory = id_factory
        # self.subscription_id = subscription_id

    def _make_resource_group(self,sub, resource):
        
        resource_group  = {
            "name": resource['resource_group'],
            "subscription":sub["_id"],
            "location": resource["location"],

        }
        tags = resource['tags']
        rg_properties  = {k:v for k,v in tags.items() if k in self.rg_tags_properties} 
        resource_group.update(rg_properties)       
        
        return resource_group
    def _make_resource(self, resource,resource_group):
        
        tags = resource.pop('tags')
        rg_properties  = {k:v for k,v in tags.items() if k in self.r_tags_properties} 
        extra_info =  {k:v for k,v in tags.items() if k not  in      self.r_tags_properties} 
        resource.update(rg_properties)
        # slots/staging 
        resource["name"] = resource['az_id'].replace("/slots/staging","").rsplit("/",1)[1]

        resource['resource_group']  = resource_group['_id']
        # resource['rocket_id'] = "{0}/{1}/{2}/{3}".format(resource_group['subscription'],resource_group['name'],resource['service_name'],resource['name'])
        if extra_info:
            resource['extra_info']  = extra_info
        return resource
    def _merge(self,principal_obj,secondary_obj):
        
        principal_obj.update(secondary_obj)
        if 'extra_info' in secondary_obj:
            extra_info = principal_obj.get('extra_info',{})
            extra_info.update(secondary_obj['extra_info'])
            principal_obj['extra_info'] = extra_info
        if "sevice_name" in principal_obj:
            principal_obj.pop("sevice_name")
        if principal_obj['name']  == 'azurebackuprg_eastus_1':
            print("-----------")
            print(secondary_obj)
            print(principal_obj)
            print("-----------")
        return principal_obj
    def _merge_resource_group(self,principal_obj,secondary_obj):
        # tags = [tag for tag in secondary_obj.pop('tags') if tag not in principal_obj['tags']]
        # principal_obj 
        migrate_tags = True
        tags_labels  = self.rg_tags_properties
        for key in secondary_obj.keys():
            if key in tags_labels and key  in principal_obj:
                migrate_tags = False
        if not migrate_tags :
            secondary_obj = {key:value for key,value in secondary_obj.items() if key not in  tags_labels}
        return self._merge(principal_obj,secondary_obj)
        
    def _merge_resource(self,principal_obj,secondary_obj):
        # tags = [tag for tag in secondary_obj.pop('tags') if tag not in principal_obj['tags']]
        # principal_obj 
        tags_labels  = self.r_tags_properties
        migrate_tags = True
        for key in secondary_obj.keys():
            if key in tags_labels and key  in principal_obj:
                migrate_tags = False
        if not migrate_tags :
            secondary_obj = {key:value for key,value in secondary_obj.items() if key not in  tags_labels}
        return self._merge(principal_obj,secondary_obj)
        
        # resource_group_id = principal_obj.pop('_id')
    def group_resources(self,preprocessed_resources):
        '''this function groups the records, of preprocessed resources, by same az_id'''
        preprocessed_resources.sort(key=lambda x:x['az_id'])
        group = [preprocessed_resources[0]]
        actual_az_id = preprocessed_resources[0]['az_id']
        len_resources   = len(preprocessed_resources)

        for index in range(1,len_resources):
            resource = preprocessed_resources[index]

            
            if resource['az_id'] == actual_az_id:
                group.append(resource)
            else:
               
                yield group
                # enter_step = False
               
                group = [resource]  
                if len_resources > index+1:
                    # actual_az_id = preprocessed_resources[index+1]['az_id']
                    actual_az_id = resource['az_id']
                else:
                    yield group
    def select_dominant_resource(self,group):
        from difflib import SequenceMatcher

        def similar_string(a, b):
            '''Calcula el procentaje de similitud de dos strings'''
            return SequenceMatcher(None, a, b).ratio()
        ### Determinar el Service name dominante
        
        if group[0]['type'] == "microsoft.web/sites":
            dominant = group[0]
            dominant['service_name']  =  "azure app service"
        elif len(group)==1:
            dominant=group[0]
            
        else:
            dominant_percents =  list(map(lambda x: (x,similar_string(x['service_name'],group[0]['type'])),group))

            dominant_percents.sort(key=lambda a:a[1],reverse=True)
            dominant=dominant_percents[0][0]  
        # dominants = open_file("./assets/azure_responses/dominants.json")    or []
        # dominants.append({'r':group,'d':dominant})
        # save_obj(dominants,"./assets/azure_responses/dominants.json")

        return dominant

    def process(self,subscription_id,preprocessed_resources):
        sub = next(sub for sub in settings.subscriptions if sub['subscription_id'] == subscription_id)
        print("starting ... ")
        groups= self.group_resources(preprocessed_resources)
        groups = list(groups)
        print(len(groups))
        # print(groups)
        # groups.sort(key = lambda x:len(x),reverse=True)
        # save_obj(groups,f"./assets/groups_{sub['env']}.json")
        duplicate_keys = []
        aresources = []
        for group in groups:
            resource = self.select_dominant_resource(group)
            
            # print(resource['az_id'])
            resource_group = self._make_resource_group(sub,resource)
            # print("pipeline ",{"name":resource_group['name'],"subscription":resource_group['subscription']})
            
            # TODO rehacer esta logica resource_group['name'] cannot be empty
            d_resource_group = None
            if  resource_group['name']:
                d_resource_group = settings.local_db.resource_groups.find_one({"name":resource_group['name'],"subscription":resource_group['subscription']})
            if d_resource_group is None: 
                print(f"rg {resource_group['name']} not found, creating ...")
                resource_group['base_id'],resource_group['short_id'] = self.id_factory.make_id(env=sub['env'],length  = 8,respose_base_id=True)
                
                resource_group_id = settings.local_db.resource_groups.insert_one(resource_group).inserted_id
                d_resource_group = {"_id":resource_group_id,**resource_group}
            else:
                # print(f"updating rg {resource_group['name']}  ...")
                # print("before")
                # print(d_resource_group)
                d_resource_group  = self._merge_resource_group(d_resource_group,resource_group)
                resource_group_id = d_resource_group.pop('_id')
                # print("after")
                # print(d_resource_group)

                # settings.local_db.resource_groups.update_one({"_id":resource_group_id},{"$set":d_resource_group})
                d_resource_group['_id'] = resource_group_id

                
            resource = self._make_resource(resource,d_resource_group)
            # d_resource = settings.local_db.resources.find_one({"name":resource['name'],"resource_group":resource['resource_group']})
            
            d_resource = settings.local_db.resources.find_one({"az_id":resource['az_id']})
           
            if d_resource is None:
                print(f"resource {resource['name']} not found, creating ...")
                resource['base_id'],resource['short_id'] = self.id_factory.make_id(env=sub['env'],length  = 12,respose_base_id=True)
                if "rocket_id" in resource:resource.pop("rocket_id")
                try:
                    resource_id = settings.local_db.resources.insert_one(resource).inserted_id
                except pymongo.errors.DuplicateKeyError as e:
                    print(resource)
                    duplicate_keys.append({'a':'a','r':resource,'e':str(e)})
                    pass
            else:
                # print(f"updating rg {resource['name']}  ...")
                # print("before")
                # print(d_resource)

                # extra_info = d_resource['extra_info']
                # extra_info.update(resource['extra_info'])
                # d_resource.update(resource)
                # d_resource['extra_info'] = extra_info
                d_resource = self._merge_resource(d_resource,resource)
                resource_id = d_resource.pop('_id')
                # print("after")
                # print(d_resource)
                try:
                    settings.local_db.resources.update_one({"_id":resource_id},{"$set":d_resource})
                except pymongo.errors.DuplicateKeyError as e:
                    print(f"duplicate key {resource}")
                    duplicate_keys.append({'a':'u','r':d_resource,'e':str(e)})

                    settings.local_db.resources.delete_one({"_id":resource_id})
            aresources.append(d_resource)

        # save_obj(aresources,f"./assets/resources_fromcosts_{sub['env']}.json",default=str)
        # save_obj(duplicate_keys,"./assets/azure_responses/duplicate_keys.json",default=str)
            # input("press enter..")
# subscription_id="b2fd9f8c-0ed5-4f6e-9c93-75ae90718afa"
# subscription_id="817e1f21-9d87-4bd6-bc04-cd55f28ae39b"
# subscription_id =  "7fefb755-c8be-4623-8ecd-b0d39089f788" #1-9
subscription_id="cd92b809-fa0a-46d9-90d7-fd06550e06da"
# subscription_id="8cd001c1-67fd-4553-b7a9-a25d59208462"

# resources  = open_file("./assets/azure_responses/preprocessed/qa(2022, 1)_(2022, 8).json")
resources  = open_file("./assets/azure_responses/preprocessed/ch(2022, 4)_(2022, 9).json")
# resources  = open_file("./assets/azure_responses/preprocessed/mark(2022, 4)_(2022, 9).json")

# resources  = open_file("./assets/azure_responses/preprocessed/prod(2022, 1)_(2022, 8).json")

# subscription_id =  "7fefb755-c8be-4623-8ecd-b0d39089f788"
# resources  = open_file(".     ")

# first_period,last_period = (2022,1),(2022,6)
from resourses_management.ids import RandomIdMaker

processor = ResourcesInCostsProcessor(RandomIdMaker())
# resources = processor._normalize_resources(subscription_id, first_period,last_period)
processor.process(subscription_id,resources)

# for group in processor.group_resources(resources):
#     # print(group)
#     for resource in group:
#         print(resource['name'])
#     if len(group)>1:
#         input()
# groups = list(processor.group_resources(resources))
# groups.sort(key=lambda x:len(x),reverse=True)
# save_obj(groups,"./assets/groups.json")

# resources = processor.process(subscription_id, first_period,last_period)
# types = []
# for resource in resources:
#     if resource['type'] not in types:
#         types.append(resource['type'])
    
# resources = list(resources)
# print(len(resources))
# print(len(types))
# save_obj(types,"types.json")
# save_obj(resources,"resources_in_costs.json")

# TODO [] realmente importante important

# // determinar cuales son los tipos domiantes y sobre cuales recursos
# //  usar los mismos costos de azure para encontrar esta relacion
# //  se pueden agrupar los costos de azure por id(se deja el tipo dominante)
# //  generar un AzuCostsProccesor para mandar los costos a la db
# //  generar una base de datos temporal con los costos de los primeros 15 dias del mes pasado y este
# // para entregarle el reporte a Jhon
# // Se supone que la parte que genera el excel debe funcionar sin ningun problema

# Se encontro que  no hay forma de relacionar los services names
# para poder mapear los costos de rocket hay que guiarse de los costos de azure con los regursos segregados(se necesita el reporte extendido)


# por otro lado para extraer los recursos de los reportes de azure




######################################333
# Hay tres cosas a terner en el procesamiento de los costos

# 1. Registrar los recursos
    # a. Del api de azure, consultar recursos existentes
    # b. Del api de azure, Microsoft.CostManagement/ informacion que viene cuando se consultan los costos
# La mayoriade la informacion de los recursos viene del inciso b
# Vienen recursos repetidos pero con diferente service name, por ende hay que determinar cual es el
# service name dominante, para registrar ese en el recurso(ya se tiene un script que extrae esa informacion).

# 2. Generar los costos de azure
#  Para esto se debe agrupar cada registro del api de azure por az_id, tambien tener en cuenta que los que tienen /slot/stagin al final, son parte de otro recurso
#  solo se quita /slots/stagin del id y listo

# 3. Mapear costos de Rocket, si o si se necesita La respuesta del api de azure de CostManagement/(1b), para poder mapear los recursos
#    se necesitan para poder obtener el az_id del recurso y poder continuar con el proceso 


#  -------------- Proceso -------------

# 1. [*] Descargar costos de azure Microsoft.CostManagement
# 2. [ ] Hacer un preprocesamiento, para quitar informacion repetida e informacion que no se necesita
#    Posteriormente esto sera usado en todos los tres modulos

# 3.  Extraer los recursos, determinar el Service Name dominante y registrar los registros en la db
# 5.  Procesar y guardar los costs de azure en la db
# 6.  Procesar y guardar los costs de rocket en la db

