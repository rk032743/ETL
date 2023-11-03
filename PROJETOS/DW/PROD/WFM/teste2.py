# string representation of list to list using json.loads()
import json
 
# initializing string representation of a list
ini_list = "[1, 2, 3, 4, 5]"
 
# printing initialized string of list and its type
print("initial string", ini_list)
print(type(ini_list))
 
# Converting string to list
res = json.loads(ini_list)
 
# printing final result and its type
print("final list", res)
print(type(res))