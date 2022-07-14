from elasticsearch6 import Elasticsearch
import requests


def connect_elasticsearch(host, port):
    es = Elasticsearch([{'host': host, 'port': port}])
    if es.ping():
        print('Connect')
    else:
        print('Not connect!')
    return es

if __name__ == '__main__':
  connect_elasticsearch('localhost', 9200)
  res = requests.get('http://localhost:9200')
  print(res.content)
