import requests 


post_url = 'https://petstore.swagger.io/v2/pet'
header = {
    'accept': 'application/json', 
    'Content-Type': 'application/json'
}
body = {
  "id": 9843212,
  "category": {
    "id": 0,
    "name": "string"
  },
  "name": "snake",
  "photoUrls": [
    "string"
  ],
  "tags": [
    {
      "id": 0,
      "name": "string"
    }
  ],
  "status": "available"
}

p = requests.post(url=post_url, headers=header, json=body)
print(p.status_code)

url = 'https://petstore.swagger.io/v2/pet/9843217/uploadImage'
file = {'file': open('./test_image.jpg', 'rb')}

r = requests.post(url,files=file)
print(r.json()['message'])