from elasticsearch6 import Elasticsearch
import os
import argparse


def arg_parse():
    ap = argparse.ArgumentParser()
    ap.add_argument("command")
    ap.add_argument("second_command", nargs='?', default=None)
    ap.add_argument("-p", "--port", type=int, default=9200)
    ap.add_argument("-u", "--host", type=str, default='localhost')
    ap.add_argument("-a", "--author")
    ap.add_argument("-y", "--year")
    ap.add_argument("-n", "--name")

    return ap.parse_args()


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
    files = os.listdir(f'input/{path}')
    for file in files:
        name_file = file
        file = file.split('.')[0]
        file_shard = file.split(' - ')
        if len(file_shard) == 3:
            name = file_shard[0]
            author = file_shard[1]
            year = file_shard[2]
            with open(f"input/{path}/{name_file}", 'r', encoding='utf-8') as f:
                es_object.index(index=index, doc_type='document', body={
                    'title': name,
                    'author': author,
                    'year_publication': year,
                    'text': f.read()
                })


def searcher(es_object, index, search):
    res = es_object.search(index=index, body=search)
    return res


def count_books_with_words(es_object, index, word):
    body = {
        "query": {
            "bool": {
                "must": [
                    {
                        "match": {"text": f"{word}"}
                    }
                ]
            }
        }
    }

    res = searcher(es_object, index, body)
    if len(res['hits']['hits']) == 0:
        print("Not found for this word")
        exit(0)
    print(f"Found: {len(res['hits']['hits'])}")
    for record in res['hits']['hits']:
        print(f"{record['_source']['title']}, {record['_source']['author']}, "
              f"{record['_source']['year_publication']}")


def search_books(es_object, index, author, word):
    body = {
        "query": {
            "bool": {
                "must": [
                    {
                        "match": {"text": f"{word}"}
                    },
                    {
                        "match": {"author": f"{author}"}
                    }
                ]
            }
        }
    }
    res = searcher(es_object, index, body)
    if len(res['hits']['hits']) == 0:
        print("Not found for this word and author")
        exit(0)
    print(f"Found: {len(res['hits']['hits'])}")
    for record in res['hits']['hits']:
        print(f"{record['_source']['title']}, {record['_source']['author']}, {record['_source']['year_publication']}")


def search_date(es_object, index, from_date, until_date, word):
    body = {
        "query": {
            "bool": {
                "must": [
                    {
                        "range": {
                            "year_publication": {
                                "gte": from_date,
                                "lte": until_date
                            }
                        }
                    }

                ],
                "must_not": {
                    "match": {"text": f"{word}"}
                }
            }
        }
    }
    res = searcher(es_object, index, body)
    if len(res['hits']['hits']) == 0:
        print("Not found for this word and date range")
        exit(0)
    print(f"Found: {len(res['hits']['hits'])}")
    for record in res['hits']['hits']:
        print(f"{record['_source']['title']}, {record['_source']['author']}, "
              f"{record['_source']['year_publication']}")


if __name__ == '__main__':
    args = arg_parse()
    index_name = 'test'
    es = connect_elasticsearch('localhost', 9200)
    #add_book('voyna-i-mir.txt', es, index_name, args.name, args.author, args.year)
    #add_books('books', es, index_name)
    #count_books_with_words(es, index_name, 'известием')
    #search_books(es, index_name, 'Толстой', 'Шерер')
    #search_date(es, index_name, 1836, 1836, 'фцаыцам')
    if args.command == 'create':
        create_index(es, index_name)
        exit(0)
    elif args.command == 'add-book':
        if args.second_command and args.name and args.author and args.year:
            add_book(args.second_command, es, index_name, args.name, args.author, args.year)
        else:
            print("Error args")
            exit(1)
    elif args.command == 'add-books':
        if args.second_command:
            add_books(args.second_command, es, index_name)
        else:
            print("Error args")
            exit(1)
    elif args.command == 'count-books-with-words':
        if args.command_second:
            count_books_with_words(es, index_name, args.second_command)
        else:
            print("No word")
            exit(1)
    elif args.command == 'search-books':
        if args.command_second and args.author:
            search_books(es, index_name, args.author, args.second_command)
        else:
            print("Error args")
            exit(1)