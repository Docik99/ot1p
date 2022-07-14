from elasticsearch6 import Elasticsearch
import requests


def connect_elasticsearch(host, port):
    es = Elasticsearch([{'host': host, 'port': port}])
    if es.ping():
        print('Connect')
    else:
        print('Not connect!')
    return es


def create_index(es_object, index_name):
    created = False
    body_books = {
        "settings": {
            "analysis": {
                "filter": {
                    "russian_stop": {
                        "type": "stop",
                        "stopwords": "_russian_"
                    },
                    "russian_keywords": {
                        "type": "stop",
                        "stopwords": ["князь", "повезет", "сорок"]
                    }
                },

                "analyzer": {
                    "custom_analyzer": {
                        "type": "custom",
                        "tokenizer": "standard",
                        "filter": [
                            "lowercase",
                            "russian_stop",
                            "russian_keywords"
                        ]
                    }
                }
            }
        },
        "mappings": {
            "document": {
                "properties": {
                    "title": {
                        "type": "text",
                        "analyzer": "standard",
                        "search_analyzer": "standard"
                    },
                    "author": {
                        "type": "text",
                        "analyzer": "standard",
                        "search_analyzer": "standard"
                    },
                    "year_publication": {
                        "type": "date",
                        "format": "yyyy"
                    },
                    "text": {
                        "type": "text",
                        "analyzer": "custom_analyzer",
                        "search_analyzer": "custom_analyzer"
                    }
                }
            }
        }
    }

    try:
        if not es_object.indices.exists(index_name):
            es_object.indices.create(index=index_name, ignore=400, body=body_books)
            print(f"Индекс: '{index_name}' успешно создан!")
            created = True
        else:
            print(f"Индекс: '{index_name}' уже существует!")
    except Exception as ex:
        print(str(ex))
    finally:
        return created

if __name__ == '__main__':
    index_name = 'test'
    es = connect_elasticsearch('localhost', 9200)
    create_index(es, index_name)
