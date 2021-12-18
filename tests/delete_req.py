import requests
from random import random, randint
import sys

url = 'http://localhost:8080/operations'
myobj = {"Type": "d", "Id": int(sys.argv[1]), "Encoding": []}

x = requests.post(url, json=myobj)

print(x.status_code)
