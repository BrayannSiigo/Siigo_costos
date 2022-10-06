import json
from typing import final
from unicodedata import normalize


class TagsProcessor:
    def __init__(self):
        self.tags_labels = {
            'owner': ['owner','onwer','responsable'],
            'tribu':['tribu','team','equipo'],
            'description':['description','descripcion'],
            'creation_date':['date','fecha','creation_date'],
            'env':['env','environment'],
            'access_level':['access_level','nivelacceso'],
            'solution':['solution','solucion'],
            'mail':['mail','email'], # TODO El email se guarda?, deberia existir un esquema de tribus e integrantes con su respectivo email?
            'country':['pais','country'],
            'product':['product']
        }
    
    def process_product(self,value):
        final_obj = []
        try:
            oldv=  value
            value = value.encode('ascii',errors='ignore').decode()
            obj = json.loads(value)
        except Exception as e:
            print(e)
            print(f"hay un error en el value product `{oldv}` `{value}`")
            if value != "b'?'":
                exit(1)
            else:
                return
        for item in obj:
            for product,subitem in item.items():
                for subi in subitem:
                    subproduct = list(subi.keys())[0]
                    percent = list(subi.values())[0]

                    # input(f"subitem {subsubitem}")
                    final_obj.append({'Product':product,'SubProduct':subproduct,'Percent':percent})
        return final_obj
    def process_tags(self,tags):
        proccesed_tags ={}
        for tag,value in tags.items():
            tag  = self.normalize_str(tag)
            found =  False
            for final_tag, coincidences in self.tags_labels.items():
                if tag in coincidences:
                    custom_processor = getattr(self,f'process_{final_tag}',lambda x:x)
                    final_tag_value =  custom_processor(value)
                    if final_tag:
                        proccesed_tags[final_tag] = final_tag_value
                        found = True
                    break
                
            if not found:
                proccesed_tags[tag] = value
        return proccesed_tags
    def normalize_str(self,s):
        trans_tab = dict.fromkeys(map(ord, u'\u0301\u0308'), None)
        return normalize('NFKC', normalize('NFKD', s).translate(trans_tab)).lower().strip()

