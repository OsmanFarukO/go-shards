import requests
from random import random, randint
import sys

url = 'http://localhost:8080/operations'
myobj = {"Type": "u", "Id": int(sys.argv[1]), "Encoding": [random() for i in range(512)]}

x = requests.post(url, json=myobj)

print(x.status_code)
