

'''
https://stackoverflow.com/questions/13484726/safe-enough-8-character-short-unique-random-string
'''

import string
import secrets
import base64
class IdsFactory:
    def __init__(self,):
        self.alphanumeric = "~!@#$%^&*+|_-.'(),"

        self.alphabet = string.ascii_letters + string.digits + self.alphanumeric

    def make_id(self,n):
        return ''.join(secrets.choice(self.alphabet) for _ in range(n))
    def make_encoded_id(self,n):
        # n have to be 4  div
        n_secret = self.roud_to_ceil_even(n,4)*3
        # print('secret length',n_secret)
        secret = self.make_id(n_secret)
        # print('secret',secret)
        encoded_id = base64.urlsafe_b64encode(bytes(secret,'utf-8'))
        return encoded_id.decode()
    def roud_to_ceil_even(self,n,div):
        result  = n//div
        if n % div != 0 :
            result+= 1
        return result

class RandomIdMaker:
    def __init__(self,length=4):
        self.ids_factory = IdsFactory()
        self.length = length
    def make_id(self,length = None,base_id = None,env='',respose_base_id=False,**kwargs):
        if length is None:
            length = self.length
        if base_id is None:
            base_id =  self.ids_factory.make_encoded_id(length)
        final_id = base_id+'_'+env if env else base_id
        if respose_base_id:
            return base_id,final_id
        else:
            return final_id