import requests
from random import random, randint
import sys

url = 'http://localhost:8080/search'
myobj = {"Encoding": [[random() for i in range(512)]]}

x = requests.post(url, json=myobj)

print(x.content)
