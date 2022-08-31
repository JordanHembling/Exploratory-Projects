import requests 
from utilities.configurations import *


se = requests.session()
token = getConfig()['API Github']['TOKEN']
print(token)
se.headers.update({'Authorization': token})

url = getConfig()['API Github']['URL']

response = se.get(url)

print(response.status_code)

repos = se.get(url + '/repos')

for i in repos.json():
    print(i['name'])