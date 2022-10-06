import json
def save_obj(vdict,name,default=None):
        try:
            with open(name, 'w', encoding='utf-8') as f:
                json.dump(vdict, f, ensure_ascii=False, indent=4,default=default)
        except Exception as e:
            print(e)
            pass
def open_file(name):
    try:
        return json.load(open(name))
    except Exception as e:
        pass