import requests 
from utilities.configurations import *


se = requests.session()
se.headers.update({'Authorization': 'Bearer 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'})

url = getConfig()['API Github']['URL']

response = se.get(url)

print(response.status_code)

repos = se.get(url + '/repos')

for i in repos.json():
    print(i['name'])