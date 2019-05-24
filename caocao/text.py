# encoding : utf-8
# @Time    : 5/22/19 5:42 PM
# @Author  : magic
# @Email   : 
# @File    : text.py
# @Software: PyCharm
import json
date=[{"Type":"GiveGold","Data":{"old":553831,"new":555359,"give":1528}},\
      {"Type":"AddProp","Data":{"id":20008,"ct":1,"old":2,"new":3}},\
      {"Type":"AddProp","Data":{"id":20192,"ct":1,"old":17,"new":18}}]


js_data =json.dumps(date)
print(js_data)
