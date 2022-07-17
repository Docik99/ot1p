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


def arg_parse():
    """Обработка аргументов командной строки

    Возвращаемые значения:
        ap: введенные аргументы
    """
    ap = argparse.ArgumentParser()
    ap.add_argument("command")
    ap.add_argument("second_command", nargs='?', default=None)
    ap.add_argument("-p", "--port", type=int, default=9200)
    ap.add_argument("-s", "--host", type=str, default='localhost')
    ap.add_argument("-a", "--author")
    ap.add_argument("-y", "--year")
    ap.add_argument("-n", "--name")
    ap.add_argument("-f", "--from_date")
    ap.add_argument("-u", "--until_date")

    return ap.parse_args()


def connect_elasticsearch(host, port):
    """Подключение к Elasticsearch

    Аргументы:
        host: имя хоста для подключения
        port: порт для подключения

    Возвращаемые значения:
        es: объект подключения

    """
    es = Elasticsearch([{'host': host, 'port': port}])
    if es.ping():
        print('Connect')
    else:
        print('Not connect!')
    return es


def create_index(es_object, index):
    """Создание индекса

    Аргументы:
        es_object: объект подключения
        index: название индекса

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


def exists(es_object, index, name, author, year):
    """Проверка существования книги

    Аргументы:
        es_object: объект подключения
        index: название индекса
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
    res = searcher(es_object, index, body)
    if len(res['hits']['hits']) == 0:
        exist = False
    return exist


def add_book(file, es_object, index, name, author, year):
    """Загрузка заданного файла

    Аргументы:
        file: название файла
        es_object: объект подключения
        index: название индекса
        name: название книги
        author: автор
        year: год публикации

    Возвращаемые значения:

    """
    if not exists(es_object, index, name, author, year):
        with open(f"input/{file}", 'r', encoding='utf-8') as f:
            es_object.index(index=index, doc_type='document', body={
                'title': name,
                'author': author,
                'year_publication': year,
                'text': f.read()
            })
        print('Успешно!')
    else:
        print('Данная книга уже существует!')


def add_books(path, es_object, index):
    """Загрузка всех файлов в каталоге

    Аргументы:
        path: путь к каталогу
        es_object: объект подключения
        index: название индекса

    Возвращаемые значения:

    """
    files = os.listdir(f'input/{path}')
    for file in files:
        name_file = file
        file = file.split('.')[0]
        file_shard = file.split(' - ')
        if len(file_shard) == 3:
            name = file_shard[0]
            author = file_shard[1]
            year = file_shard[2]
            if not exists(es_object, index, name, author, year):
                with open(f"input/{path}/{name_file}", 'r', encoding='utf-8') as f:
                    es_object.index(index=index, doc_type='document', body={
                        'title': name,
                        'author': author,
                        'year_publication': year,
                        'text': f.read()
                    })
            else:
                print(f"Эта книга уже существует: {name_file.split('.')[0]}")
    print('Загрузка завершена!')


def searcher(es_object, index, search):
    """Поиск в индексе по определенному запросу

    Аргументы:
        es_object: объект подключения
        index: название индекса
        search: запрос

    Возвращаемые значения:
        res: результат поиска
    """
    res = es_object.search(index=index, body=search)
    return res


def count_books_with_words(es_object, index, word):
    """Вывод всех книг с заданным словом

    Аргументы:
        es_object: объект подключения
        index: название индекса
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

    res = searcher(es_object, index, body)
    if len(res['hits']['hits']) == 0:
        print("Not found for this word")
        exit(0)
    print(f"Found: {len(res['hits']['hits'])}")
    for record in res['hits']['hits']:
        print(f"{record['_source']['title']}, {record['_source']['author']}, "
              f"{record['_source']['year_publication']}")


def search_books(es_object, index, author, word):
    """Вывод всех книг заданного автора, которые содержат заданную строку

    Аргументы:
        es_object: объект подключения
        index: название индекса
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
    res = searcher(es_object, index, body)
    if len(res['hits']['hits']) == 0:
        print("Not found for this word and author")
        exit(0)
    print(f"Found: {len(res['hits']['hits'])}")
    for record in res['hits']['hits']:
        print(f"{record['_source']['title']}, {record['_source']['author']},"
              f" {record['_source']['year_publication']}")


def search_date(es_object, index, from_date, until_date, word):
    """Вывод всех книг из указанного диапазона годов, которые НЕ содержат заданную строку

    Аргументы:
        es_object: объект подключения
        index: название индекса
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
    res = searcher(es_object, index, body)
    if len(res['hits']['hits']) == 0:
        print("Not found for this word and date range")
        exit(0)
    print(f"Found: {len(res['hits']['hits'])}")
    for record in res['hits']['hits']:
        print(f"{record['_source']['title']}, {record['_source']['author']}, "
              f"{record['_source']['year_publication']}")


def calc_date(es_object, index, author):
    """Вывод среднего арифметического для года издания заданного автора

    Аргументы:
        es_object: объект подключения
        index: название индекса
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
    res = searcher(es_object, index, body)
    if len(res['hits']['hits']) == 0:
        print("Not found for this author")
        exit(0)
    years = []
    for record in res['hits']['hits']:
        years.append(int(record['_source']['year_publication']))
    print(round(mean(years)))


def search_by_year(es_object, index, year):
    """Поиск книг по году

    Аргументы:
        es_object: объект подключения
        index: название индекса
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
    res = searcher(es_object, index, body)
    if len(res['hits']['hits']) == 0:
        print("Not found for this year")
        exit(0)
    ids = []
    for record in res['hits']['hits']:
        ids.append(record['_id'])
    return ids


def top_words(es_object, index, year):
    """Вывод топ-10 самых популярных слов с количеством их упоминаний во всех книгах одного года

    Аргументы:
        es_object: объект подключения
        index: название индекса
        year: год публикации

    Возвращаемые значения:

    """
    terms = {}
    body = {
        "fields": ["text"]
    }

    ids = search_by_year(es_object, index, year)

    for idi in ids:
        res = es_object.termvectors(index=index, doc_type="document", id=idi, body=body)
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
    index_name = '2018-3-09-doc-lr2'
    es = connect_elasticsearch('localhost', 9200)
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
        if args.second_command:
            count_books_with_words(es, index_name, args.second_command)
        else:
            print("No word")
            exit(1)
    elif args.command == 'search-books':
        if args.second_command and args.author:
            search_books(es, index_name, args.author, args.second_command)
        else:
            print("Error args")
            exit(1)
    elif args.command == 'search-dates':
        if args.from_date and args.until_date and args.second_command:
            search_date(es, index_name, args.from_date, args.until_date, args.second_command)
        else:
            print("Error args")
            exit(1)
    elif args.command == 'calc-date':
        if args.author:
            calc_date(es, index_name, args.author)
        else:
            print("Error args")
            exit(1)
    elif args.command == 'top-words':
        if args.year:
            top_words(es, index_name, args.year)
        else:
            print("Error args")
            exit(1)
    else:
        print("Unknown command")
        exit(1)


if __name__ == '__main__':
    main()
