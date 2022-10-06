
from fileinput import filename
from time import time
from dict_json import open_file,save_obj
from core import settings
from  resourses_management.tags_processor import TagsProcessor

def fix_az_id(az_id):
    ''''Esta funcion es una solicion temporal para los sub recursos de las  bases de datos que salen en los costos'''
    special_ids = ['/subscriptions/7fefb755-c8be-4623-8ecd-b0d39089f788/providers/microsoft.sql/locations/eastus2/longtermretentionservers/azsql-colombia-01',
    '/subscriptions/7fefb755-c8be-4623-8ecd-b0d39089f788/providers/microsoft.sql/locations/eastus2/longtermretentionservers/azsql-colombia-02',
    '/subscriptions/7fefb755-c8be-4623-8ecd-b0d39089f788/providers/microsoft.sql/locations/eastus2/longtermretentionservers/azsql-colombia-03',
    '/subscriptions/7fefb755-c8be-4623-8ecd-b0d39089f788/providers/microsoft.sql/locations/eastus2/longtermretentionservers/azsql-colombia-04',
    '/subscriptions/7fefb755-c8be-4623-8ecd-b0d39089f788/providers/microsoft.sql/locations/eastus2/longtermretentionservers/azsql-colombia-05',
    '/subscriptions/7fefb755-c8be-4623-8ecd-b0d39089f788/providers/microsoft.sql/locations/northcentralus/longtermretentionservers/azsql-colombia-06',
    '/subscriptions/7fefb755-c8be-4623-8ecd-b0d39089f788/providers/microsoft.sql/locations/northcentralus/longtermretentionservers/azsql-colombia-07']
    for special_id in special_ids:
        if special_id in az_id:
            return special_id
    return az_id

# def generate_periods(first_period,last_period):
#     last_month=-1
#     for year in range(first_period[0],last_period[0]+1):
#         first_month = 1 if last_month == 12 else first_period[1]

#         last_month = last_period[1] if year == last_period[0] else 12
#         for month in range(first_month,last_month+1):
#             period = f"{year}-{'{:02d}'.format(month)}"
#             yield period

class ResourcesInCostsPreprocessor:
    '''Esta clase procesa la informacion de los recursos que se 
    encuentra en la respuesta de azure  cuando se consulta el uso de cada recurso
    '''
    def __init__(self) -> None:
        self.tagsprocessor = TagsProcessor()
       
        # self.subscription_id = subscription_id
    def generate_periods(self,first_period,last_period,reverse=False):
        '''Genera los periodos comprendidos entre <first_period y <last_period>
           first_period :: (year,month)
           last_period :: (year,month)
        '''
        last_month=-1
        years_range = range(last_period[0],first_period[0]-1,-1) if reverse else range(first_period[0],last_period[0]+1) 
        for year in years_range:
            first_month = first_period[1] if first_period[0] == year else 1

            last_month = last_period[1] if year == last_period[0] else 12
            months_range = range(last_month,first_month-1,-1) if reverse else range(first_month,last_month+1) 

            for month in months_range:
                period = f"{year}-{'{:02d}'.format(month)}"
                yield period


    def extract_resources(self,subscription_id, first_period,last_period):
        sub = next(sub for sub in settings.subscriptions if sub['subscription_id'] == subscription_id)
        print(sub)
        '''Esta clase recoge los costos de varios periodos y  extrae la informacion de los
        recursos listados'''
        proccesed_ids = []
        costs=[]
        # timePeriods = [
        #     {'from': '2022-01-01T00:00:00', 'to': '2022-01-15T23:59:59'},
        #     {'from': '2022-02-01T00:00:00', 'to': '2022-02-14T23:59:59'},
        #     {'from': '2022-03-01T00:00:00', 'to': '2022-03-15T23:59:59'},
        #     {'from': '2022-04-01T00:00:00', 'to': '2022-04-15T23:59:59'},
        #     {'from': '2022-05-01T00:00:00', 'to': '2022-05-15T23:59:59'},
        #     {'from': '2022-06-01T00:00:00', 'to': '2022-06-15T23:59:59'},
        #     {'from': '2022-07-01T00:00:00', 'to': '2022-07-15T23:59:59'}
        # ]
        # for period in self.generate_periods(first_period,last_period,reverse=True):
        #     print(period)
        #     costs  += open_file(f"./assets/azure_responses/{sub['env']}{period}.json")
        # for timePeriod in timePeriods:
        year = 2022
        for month in range(4,9):
            # print(timePeriod)
            filename  = f"./assets/azure_responses/{sub['env']}{month}-{year}.json"
            # filename = f"./assets/azure_responses/{sub['env']}{timePeriod['from']}_{timePeriod['to']}.json"
            print(filename)
            costs  += open_file(filename)
        print(len(costs))
        resources = []
        for cost in costs:
            _,rg,azure_type,location,az_id,service_name,_,_,tags,_ = cost
            rg,azure_type,location,az_id,service_name = map(lambda x:x.lower(),(rg,azure_type,location,az_id,service_name))
            # if az_id not in proccesed_ids:
            
            # az_id = fix_az_id(az_id)
            resource_name =  az_id.replace("/slots/staging","").rsplit("/",1)[1]

            if not resource_name:
                settings.logger.error(f"Resource with empty name {cost}") 
            if not rg:
                settings.logger.error(f"Resource with empty resource group {cost}") 
            
            rocket_id = "{0}/{1}/{2}/{3}".format(sub['_id'],rg,service_name,resource_name)
            if rg and rocket_id not in proccesed_ids:
                dict_tags = {}
                for tag in tags:
                    key,value = map(lambda x: x.replace("\"",""),tag.split(":",1))                     
                    dict_tags[key] = value
                print(az_id)
                resource = {
                    "name" : az_id.replace("/slots/staging","").rsplit("/",1)[1],
                    "resource_group":rg,
                    "type":azure_type,
                    "location":location,
                    "az_id":az_id,
                    "rocket_id":rocket_id,
                    "service_name":service_name,
                    "tags":self.tagsprocessor.process_tags(dict_tags)
                }
                # yield resource
                resources.append(resource)
                proccesed_ids.append(rocket_id)
       

        resources.sort(key=lambda x:x["az_id"])
        # return resources
        filename=  f'./assets/azure_responses/preprocessed/{sub["env"]}{first_period}_{last_period}.json'
        save_obj(resources,filename)
        settings.logger.info(f"preprocessed resources saved in {filename}" )

    
# subscription_id="b2fd9f8c-0ed5-4f6e-9c93-75ae90718afa" # 1 - 9
# subscription_id="817e1f21-9d87-4bd6-bc04-cd55f28ae39b" # 4-9
subscription_id="cd92b809-fa0a-46d9-90d7-fd06550e06da"
# subscription_id="8cd001c1-67fd-4553-b7a9-a25d59208462"

# subscription_id =  "7fefb755-c8be-4623-8ecd-b0d39089f788" #1-9

first_period,last_period = (2022,4),(2022,9)
processor = ResourcesInCostsPreprocessor()
# resources = processor._normalize_resources(subscription_id, first_period,last_period)
resources = processor.extract_resources(subscription_id, first_period,last_period)



