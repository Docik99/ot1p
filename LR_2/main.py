"""
Скрипт для работы с elasticsearch

usage: main.py [-h] [-p PORT] [-s HOST] [-a AUTHOR] [-y YEAR] [-n NAME]
               [-f FROM_DATE] [-u UNTIL_DATE] command [second_command]

1) Создание индекса (create)
2) Загрузка заданного файла (add-book)
3) Загрузка всех файлов в каталоге (add-books)
4) Поиск всех книг с заданным словом (count-books-with-words)
5) Поиск всех книг заданного автора, которые содержат заданную строку (search-books)
6) Поиск всех книг из указанного диапазона годов, которые НЕ содержат заданную строку (search-dates)
7) Вычислить среднее арифметическое для года издания заданного автора
8) Топ-10 самых популярных слов с количеством их упоминаний во всех книгах одного года (top-words)
"""

import os
import argparse
from statistics import mean

from prettytable import PrettyTable
from elasticsearch6 import Elasticsearch


INDEX_NAME = '2018-3-09-doc-lr2'


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


def exists(es_object, name, author, year):
    """Проверка существования книги

    Аргументы:
        es_object: объект подключения
        name: название книги
        author: автор
        year: год публикации

    Возвращаемые значения:
        exist: существует ли данная книга в заданом индексе (True/False)

    """
    exist = True
    body = {
        "query": {
            "bool": {
                "must": [
                    {
                        "match": {"title": f"{name}"}
                    },
                    {
                        "match": {"author": f"{author}"}
                    },
                    {
                        "match": {"year_publication": f"{year}"}
                    }
                ]
            }
        }
    }
    res = searcher(es_object, body)
    if len(res['hits']['hits']) == 0:
        exist = False
    return exist


def add_book(file, es_object, name, author, year):
    """Загрузка заданного файла

    Аргументы:
        file: название файла
        es_object: объект подключения
        name: название книги
        author: автор
        year: год публикации

    Возвращаемые значения:

    """
    if not exists(es_object, name, author, year):
        with open(f"LR_2/input/{file}", 'r', encoding='utf-8') as f:
            es_object.INDEX_NAME(index=INDEX_NAME, doc_type='document', body={
                'title': name,
                'author': author,
                'year_publication': year,
                'text': f.read()
            })
        print('Успешно!')
    else:
        print('Данная книга уже существует!')


def add_books(path, es_object):
    """Загрузка всех файлов в каталоге

    Аргументы:
        path: путь к каталогу
        es_object: объект подключения

    Возвращаемые значения:

    """
    files = os.listdir(f'LR_2/LR_2/input/{path}')
    for file in files:
        name_file = file
        file = file.split('.')[0]
        file_shard = file.split(' - ')
        if len(file_shard) == 3:
            name = file_shard[0]
            author = file_shard[1]
            year = file_shard[2]
            if not exists(es_object, name, author, year):
                with open(f"LR_2/input/{path}/{name_file}", 'r', encoding='utf-8') as f:
                    es_object.INDEX_NAME(index=INDEX_NAME, doc_type='document', body={
                        'title': name,
                        'author': author,
                        'year_publication': year,
                        'text': f.read()
                    })
            else:
                print(f"Эта книга уже существует: {name_file.split('.')[0]}")
    print('Загрузка завершена!')


def searcher(es_object, search):
    """Поиск в индексе по определенному запросу

    Аргументы:
        es_object: объект подключения
        search: запрос

    Возвращаемые значения:
        res: результат поиска
    """
    res = es_object.search(index=INDEX_NAME, body=search)
    return res


def count_books_with_words(es_object, word):
    """Вывод всех книг с заданным словом

    Аргументы:
        es_object: объект подключения
        word: слово

    Возвращаемые значения:

    """
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

    res = searcher(es_object, body)
    if len(res['hits']['hits']) == 0:
        print("Not found for this word")
        exit(0)
    print(f"Found: {len(res['hits']['hits'])}")
    for record in res['hits']['hits']:
        print(f"{record['_source']['title']}, {record['_source']['author']}, "
              f"{record['_source']['year_publication']}")


def search_books(es_object, author, word):
    """Вывод всех книг заданного автора, которые содержат заданную строку

    Аргументы:
        es_object: объект подключения
        author: автор
        word: строка

    Возвращаемые значения:

    """
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
    res = searcher(es_object, body)
    if len(res['hits']['hits']) == 0:
        print("Not found for this word and author")
        exit(0)
    print(f"Found: {len(res['hits']['hits'])}")
    for record in res['hits']['hits']:
        print(f"{record['_source']['title']}, {record['_source']['author']},"
              f" {record['_source']['year_publication']}")


def search_date(es_object, from_date, until_date, word):
    """Вывод всех книг из указанного диапазона годов, которые НЕ содержат заданную строку

    Аргументы:
        es_object: объект подключения
        from_date: начальный год
        until_date: конечный год
        word: строка

    Возвращаемые значения:

    """
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
    res = searcher(es_object, body)
    if len(res['hits']['hits']) == 0:
        print("Not found for this word and date range")
        exit(0)
    print(f"Found: {len(res['hits']['hits'])}")
    for record in res['hits']['hits']:
        print(f"{record['_source']['title']}, {record['_source']['author']}, "
              f"{record['_source']['year_publication']}")


def calc_date(es_object, author):
    """Вывод среднего арифметического для года издания заданного автора

    Аргументы:
        es_object: объект подключения
        author: автор

    Возвращаемые значения:

    """
    body = {
        "query": {
            "bool": {
                "must": [
                    {
                        "match": {"author": f"{author}"}
                    }
                ]
            }
        }
    }
    res = searcher(es_object, body)
    if len(res['hits']['hits']) == 0:
        print("Not found for this author")
        exit(0)
    years = []
    for record in res['hits']['hits']:
        years.append(int(record['_source']['year_publication']))
    print(round(mean(years)))


def search_by_year(es_object, year):
    """Поиск книг по году

    Аргументы:
        es_object: объект подключения
        year: год публикации

    Возвращаемые значения:
        res: результат поиска

    """
    body = {
        "query": {
            "bool": {
                "must": [
                    {
                        "match": {"year_publication": year}
                    }
                ]
            }
        }
    }
    res = searcher(es_object, body)
    if len(res['hits']['hits']) == 0:
        print("Not found for this year")
        exit(0)
    ids = []
    for record in res['hits']['hits']:
        ids.append(record['_id'])
    return ids


def top_words(es_object, year):
    """Вывод топ-10 самых популярных слов с количеством их упоминаний во всех книгах одного года

    Аргументы:
        es_object: объект подключения
        year: год публикации

    Возвращаемые значения:

    """
    terms = {}
    body = {
        "fields": ["text"]
    }

    ids = search_by_year(es_object, year)

    for idi in ids:
        res = es_object.termvectors(index=INDEX_NAME, doc_type="document", id=idi, body=body)
        for term in res['term_vectors']['text']['terms'].keys():
            if term in terms.keys():
                terms[term] += res['term_vectors']['text']['terms'][term]['term_freq']
            else:
                terms[term] = res['term_vectors']['text']['terms'][term]['term_freq']

    sorted_tuples = sorted(terms.items(), key=lambda item: item[1], reverse=True)
    sorted_dict = {slovo: count for slovo, count in sorted_tuples}

    print(f"Топ-10 самых популярных слов в книгах {year} года:\n")
    head = ['Слово', 'Количество упоминаний']
    table = PrettyTable(head)
    for slovo in list(sorted_dict.keys())[0:10]:
        body = [slovo, sorted_dict[slovo]]
        table.add_row(body)
    print(table)


def main():
    """Передача аргументов командной строки исполняемым функциям"""
    args = arg_parse()
    elastic = connect_elasticsearch('localhost', 9200)
    if args.command == 'create':
        create_index(elastic)
        exit(0)
    elif args.command == 'add-book':
        if args.second_command and args.name and args.author and args.year:
            add_book(args.second_command, elastic, args.name, args.author, args.year)
        else:
            print("Error args")
            exit(1)
    elif args.command == 'add-books':
        if args.second_command:
            add_books(args.second_command, elastic)
        else:
            print("Error args")
            exit(1)
    elif args.command == 'count-books-with-words':
        if args.second_command:
            count_books_with_words(elastic, args.second_command)
        else:
            print("No word")
            exit(1)
    elif args.command == 'search-books':
        if args.second_command and args.author:
            search_books(elastic, args.author, args.second_command)
        else:
            print("Error args")
            exit(1)
    elif args.command == 'search-dates':
        if args.from_date and args.until_date and args.second_command:
            search_date(elastic, args.from_date, args.until_date, args.second_command)
        else:
            print("Error args")
            exit(1)
    elif args.command == 'calc-date':
        if args.author:
            calc_date(elastic, args.author)
        else:
            print("Error args")
            exit(1)
    elif args.command == 'top-words':
        if args.year:
            top_words(elastic, args.year)
        else:
            print("Error args")
            exit(1)
    else:
        print("Unknown command")
        exit(1)


if __name__ == '__main__':
    main()
