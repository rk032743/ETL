import os
import requests
from bs4 import BeautifulSoup

BASE_URL = 'https://latamneworg.my.salesforce.com/'
USERNAME = "giuliano.biasioli@grupokonecta.com"
PASSWORD = "Cometa10"

project_path = os.path.dirname(os.path.abspath(__file__))
html_response = os.path.join(project_path, 'index.html')

s = requests.Session()

data = {"username": USERNAME, "pw": PASSWORD}

cookies = {
    
    'sfdc-stream': '!HMLxGjJ8hUt49dpyb8gcdaVHEwp3tWKzwebmP+eGELZrooDuiO3Hmd9XYkxg/1Iujv7arpPryzjbXBg=',
    'sfdc_lv2': 'KxG83Hz2jzaSl87bbRM6Mjws35nLw4mOGfoZk9pSTsdSpc6JItx5Wv/hvPzjWI5UQ=',
}


data = {
   
    'username': 'giuliano.biasioli@grupokonecta.com',
    'pw': 'Cometa10'
}
 


r = s.post(BASE_URL, data=data,cookies=cookies)

soup = BeautifulSoup(r.text, 'html.parser')
with open(html_response, 'w',  encoding='utf-8') as f:
    f.write(str(soup))
if soup.find(id="Login") is None:
	print('Successfully logged in')
else:
	print('Authentication Error')
