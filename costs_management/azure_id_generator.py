from dict_json import open_file,save_obj
special_azure_types = ['microsoft.cdn/profiles/endpoints',
 'microsoft.compute/galleries/images',
 'microsoft.compute/virtualmachines/extensions',
 'microsoft.containerregistry/registries/webhooks',
 'microsoft.network/networkwatchers/flowlogs',
 'microsoft.network/privatednszones/virtualnetworklinks',
 'microsoft.sql/managedinstances/databases',
 'microsoft.sql/servers/databases',
 'microsoft.sql/servers/elasticpools',
 'microsoft.sql/servers/jobagents',
 'microsoft.web/sites/slots',
 'microsoft.cdn/profiles/endpoints']


        
class RocketAzureIdGenerator:
    def __init__(self,subscription) -> None:
        self._resource_types_dict =  open_file("billing_fetch_core/category_types_map.json")
        self.subscription = subscription
    def resource_id_of_racket_cost(self,cost_obj):
        category = cost_obj.get("resource_category")
        resource_group = cost_obj['parent_id']
        resource_name  = cost_obj['resource_id']
        # print(category)
        if not category:
            return iter([])
        if category.lower() not in self._resource_types_dict:
            self._resource_types_dict[category.lower()] = []
        for possible_type in self._resource_types_dict[category.lower()]:
            yield self.generate_az_id(resource_group,possible_type,resource_name)
    def generate_racket_id(self,cost_obj):
        category = cost_obj.get("resource_category",'__').lower()
        resource_group = cost_obj['parent_id'].lower()
        resource_name  = cost_obj['resource_id'].lower()
        return f"{self.subscription['_id']}/{resource_group}/{category}/{resource_name}"
    def generate_az_id(self,resource_group,vtype,resource_name ):
        subscription = self.subscription["az_id"].lower()
        resource_group = resource_group.lower()
        vtype = vtype.lower()
        resource_name = resource_name.lower()

        if vtype not in special_azure_types:
            return  f"/subscriptions/{subscription}/resourcegroups/{resource_group}/providers/{vtype}/{resource_name}".lower()
        elif vtype:
            vtype,temp1 = vtype.lower().rsplit("/",1)
            temp2,temp3 = resource_name.split("/",1)
            resource_name = f"{temp2}/{temp1}/{temp3}"
            
            return self.generate_az_id(subscription,resource_group,vtype,resource_name )
    def close(self):
        save_obj(self._resource_types_dict,"./category_types_map.json")