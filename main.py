from elasticsearch6 import Elasticsearch
import os


def connect_elasticsearch(host, port):
    es = Elasticsearch([{'host': host, 'port': port}])
    if es.ping():
        print('Connect')
    else:
        print('Not connect!')
    return es


def create_index(es_object, index):
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
        if not es_object.indices.exists(index):
            es_object.indices.create(index=index, ignore=400, body=body_books)
            print(f"Индекс: '{index}' успешно создан!")
            created = True
        else:
            print(f"Индекс: '{index}' уже существует!")
    except Exception as ex:
        print(str(ex))
    finally:
        return created


def add_book(file, es_object, index, name, author, year):
    with open(f"input/{file}", 'r', encoding='utf-8') as f:
        es_object.index(index=index, doc_type='document', body={
            'title': name,
            'author': author,
            'year_publication': year,
            'text': f.read()
        })


def add_books(path, es_object, index):
    files = os.listdir(path)
    for file in files:
        name_file = file
        file = file.split('.')[0]
        file_shard = file.split('-')
        if len(file_shard) == 3:
            name = file_shard[0].replace(' ', '')
            author = file_shard[1]
            year = file_shard[2].replace(' ', '')
            with open(f"{path}/{name_file}", 'r', encoding='utf-8') as f:
                es_object.index(index=index, doc_type='document', body={
                    'title': name,
                    'author': author,
                    'year_publication': year,
                    'text': f.read()
                })


def searcher(es_object, index, search):
    res = es_object.search(index=index, body=search)
    return res


if __name__ == '__main__':
    index_name = 'test'
    es = connect_elasticsearch('localhost', 9200)
