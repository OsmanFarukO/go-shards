import requests
from random import random, randint

url = 'https://go-shard.k8s.cluster/operations'
myobj = {"Type": "i", "Id": randint(123, 123456), "Encoding": [random() for i in range(512)]}

x = requests.post(url, json=myobj)

print(x.status_code)
