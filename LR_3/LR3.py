import os
import sys
import argparse
from statistics import mean

from prettytable import PrettyTable
from elasticsearch6 import Elasticsearch


INDEX_NAME = 'LR3'


def arg_parse():
    """Обработка аргументов командной строки

    Возвращаемые значения:
        argument: введенные аргументы
    """
    argument = argparse.ArgumentParser()
    argument.add_argument("command")
    argument.add_argument("second_command", nargs='?', default=None)
    argument.add_argument("-p", "--port", type=int, default=9200)
    argument.add_argument("-s", "--host", type=str, default='localhost')
    argument.add_argument("-a", "--author")
    argument.add_argument("-y", "--year")
    argument.add_argument("-n", "--name")
    argument.add_argument("-f", "--from_date")
    argument.add_argument("-u", "--until_date")

    return argument.parse_args()


def connect_elasticsearch(host, port):
    """Подключение к Elasticsearch

    Аргументы:
        host: имя хоста для подключения
        port: порт для подключения

    Возвращаемые значения:
        elastic: объект подключения

    """
    elastic = Elasticsearch([{'host': host, 'port': port}])
    if elastic.ping():
        print('Connect')
    else:
        print('Not connect!')
    return elastic


def create_index(es_object):
    """Создание индекса

    Аргументы:
        es_object: объект подключения

    Возвращаемые значения:
        created: был ли создан новый индекс (True/False)

    """
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
        if not es_object.indices.exists(INDEX_NAME):
            es_object.indices.create(index=INDEX_NAME, ignore=400, body=body_books)
            print(f"Индекс: '{INDEX_NAME}' успешно создан!")
            created = True
        else:
            print(f"Индекс: '{INDEX_NAME}' уже существует!")
    except TypeError as ex:
        print(str(ex))

    return created