from elasticsearch6 import Elasticsearch
import os


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


def add_book(file, es_object, name, author, year):
    with open(f"input/{file}", 'r', encoding='utf-8') as f:
        es_object.index(index=index_name, doc_type='document', body={
            'title': name,
            'author': author,
            'year_publication': year,
            'text': f.read()
        })


if __name__ == '__main__':
    index_name = 'test'
    es = connect_elasticsearch('localhost', 9200)
    create_index(es, index_name)
    add_book('voyna-i-mir.txt', es, 'ВойнаИМир', 'Tolstoy', 1865)
