from elasticsearch import Elasticsearch
import requests


def connect_elasticsearch():
    es = Elasticsearch('http://localhost:9200')
    if es.ping():
        print('Yay Connect')
    else:
        print('Awww it could not connect!')
    return es

if __name__ == '__main__':
  connect_elasticsearch()
  res = requests.get('http://localhost:9200')
  print(res.content)
