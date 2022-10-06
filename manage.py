'''
Author: Santiago Andres Ramirez
Este archivo agrega a ~/.bashrc y por ende se ejecutara todos los dias, su objetivo es:
1. [ * ] Ejectutar el resources fetcher
2. [ ] Ejecutar el billing fetcher, este hace fetch de los costos del mes inmediatamente anterior,
       
       Los costos son mapeados con la informacion de la base de datos local pero almacenados remotamente
       en la db de produccion
4. [ ] Cuando si se generan registros de costos se sincronizan los recursos a la base de datos remota
3. [ ] Paralelamente se lleva una gestion de sincronizacion con la base dedatos remota,
       Esta verifica si la informacion de costos ya esta en produccion(para no subirla dos veces), ademas de elminiar la 
       informacion correcta cuando ha ocurrido un error y no se completo una sincronizacion correctamente

'''
import os
import datetime
from dateutil.relativedelta import relativedelta
from resourses_management.main import fetch as resources_fetch
from  costs_management.main import fetch_costs
from sync import sync_resource_groups,sync_resources,query_resent_sync_record
import core.settings as settings
def main(provider,month=None,year=None):
    if not month or not year:
        now_date =  datetime.datetime.now(tz= settings.timezone) - relativedelta(months=1)
        year = now_date.year
        month = now_date.month
        now_date = now_date.isoformat()
    else:
        now_date = f"{month}-{year}"
    
    # subscriptions =  os.environ["subscriptions"]
    subscriptions = filter(lambda x: x,os.environ.get("subscriptions","").split("/"))

    if not subscriptions or not month or not year:
        raise Exception('subscriptions,month,year are required')


    resent_fetch = query_resent_sync_record('resources',relativedelta(hours=settings.fetch_frecuency),local=True,status="complete")
    # print(resent_fetch)
    resent_fetch = None
    if resent_fetch is None:
        # pass
        settings.logger.info(f'Local  daily {now_date} resources fetch is  starting')
        # resources_fetch(subscriptions) # local
        settings.logger.info(f'Local  daily resources fetch is  complete')

    else:
        settings.logger.info(f'Skipping Local  resources fetching, cause there is a resent fetch: {resent_fetch}')

         

    settings.logger.info(f'Costs {month}-{year}  fetch is starting')
    records_count = fetch_costs(year=year,month=month,provider=provider,subscriptions=subscriptions)
    settings.logger.info(f'Costs {month}-{year} Fetch is complete, {records_count} costs records already inserted')
    # if records_count:
    #     sync_resources()
    #     sync_resource_groups()
providers = ["rocket","azure"]
for provider in providers: 
# provider = "azure" 
    # main(provider,1,2022)
    # main(provider,2,2022)
    # main(provider,3,2022)
    main(provider,4,2022)
    main(provider,5,2022)
    main(provider,6,2022)
    main(provider,7,2022)
    main(provider,8,2022)
    # main(provider,9,2022)
# main()


# year = 2022
# for month in range(6,7):
#     main(month,year)