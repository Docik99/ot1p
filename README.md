Необходимо открыть терминал по адресу папки с проектом

## Создание образа

`$ docker build -t 2018-3-09-doc-lr2 .`

## Запуск образа

#### 1) Создание индекса
`$ docker run --rm --network host 2018-3-09-doc-lr2 create`

#### 2) Загрузка заданного файла
`$ docker run --rm --network host 2018-3-09-doc-lr2 add-book voyna-i-mir.txt --author Толстой --year 1865 --name 'Война И Мир'`

#### 3) Загрузка всех файлов в каталоге
`$ docker run --rm --network host 2018-3-09-doc-lr2 add-books books`

#### 4) Поиск всех книг с заданным словом
`$ docker run --rm --network host 2018-3-09-doc-lr2 count-books-with-words известием`

#### 5) Поиск всех книги заданного автора, которые содержат заданную строку
`$ docker run --rm --network host 2018-3-09-doc-lr2 search-books вином -a Пушкин`

#### 6) Поиск всех из указанного диапазона годов, которые не содержат заданную строку
`$ docker run --rm --network host 2018-3-09-doc-lr2 search-dates самолет --f 1831 --u 1836`

#### 7) Топ-10 самых популярных слов с количеством их упоминаний во всех книгах заданного года
`$ docker run --rm --network host 2018-3-09-doc-lr2 top-words --year 1836`