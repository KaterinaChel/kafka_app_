# kafka_app_final

Read me

Описание проекта
Marketplace Analytics Platform — это комплексная аналитическая платформа для маркетплейса, предназначенная для сбора, обработки и анализа данных о взаимодействии клиентов с платформой.

Цели проекта
Создание отказоустойчивой системы сбора данных от магазинов и клиентов

Реализация потоковой фильтрации запрещенных товаров

Построение Data Lake на базе HDFS для хранения данных

Обеспечение масштабируемой аналитики с использованием Apache Spark

Мониторинг и визуализация метрик производительности


Использованные технологии
 Core Infrastructure
Apache Kafka	7.0.1	Центральный брокер сообщений, обеспечивающий надежную передачу данных между сервисами
Apache Zookeeper	7.0.1	Координация и управление кластером Kafka
Kafka Connect	7.0.1	Интеграция с внешними системами и MirrorMaker 2.0
Schema Registry	7.0.1	Управление схемами Avro для сериализации данных
Apache Hadoop HDFS	3.4.1	Data Lake для хранения сырых данных
Apache Spark	3.5.4	Вычисления и аналитическая обработка больших данных
Processing Layer
Faust	Потоковая обработка и фильтрация товаров в реальном времени
Kafka-Python	Python клиент для взаимодействия с Kafka
PySpark	Python API для Spark аналитики
Watchdog	Мониторинг файловой системы для автоматической загрузки данных
 Monitoring & Visualization
Prometheus	Сбор метрик производительности
Grafana	Визуализация метрик и дашборды
JMX Exporter	Экспорт метрик Kafka в Prometheus
 Storage & Search
СSV файлы для записи данных 
HDFS
 Архитектура решения

Компоненты архитектуры
1. Кластер Source (Основной)
Брокеры: kafka-1, kafka-2, kafka-3 (3 брокера)

Топики:

shop_products — товары от магазинов

client_events — события пользователей

filtered_products — отфильтрованные товары

Безопасность: TLS/SSL, ACL, взаимная аутентификация

2. Кластер Target (Аналитический)
Брокеры: kafka-4, kafka-5, kafka-6 (3 брокера)

Топики:

source.shop_products — реплицированные товары

source.client_events — реплицированные события

3. Stream Processor (Faust)
Фильтрация товаров по списку запрещенных

Логирование разрешенных/заблокированных товаров

Управление стоп-листом через API

4. Data Lake (HDFS)
1 NameNode, 3 DataNode

Партиционирование по категориям товаров

Формат хранения: Parquet (сжатие snappy)

5. ETL Pipeline (Spark)
Чтение из Target Kafka

Трансформация и агрегация данных

Запись в HDFS партиционированными Parquet файлами

 Логика реализации
1. Shop API — Эмуляция магазинов
python
# Логика работы Shop Producer
- Мониторинг директории shop_api
- Отправка в топик shop_products

2. Client API — Эмуляция пользователей
python
# Поддерживаемые команды CLI
> search "умные часы"    # Поиск товара
> recs                   # Запрос рекомендаций
> view <product_id>      # Просмотр товара
Логика:

Интерактивный режим через терминал
Отправка событий в Kafka (client_events)


3. Stream Processor — Фильтрация запрещенных товаров
python
# Алгоритм фильтрации Faust
1. Подписка на топик shop_products
2. Для каждого товара:
   - Проверка наличия в banned_products
   - Если товар разрешен → filtered_products
   - Если запрещен → логирование и пропуск
3. Статистика: Разрешено/Заблокировано
Управление стоп-листом:

bash
curl -X POST http://localhost:6066/ban/ноутбук
curl -X DELETE http://localhost:6066/ban/ноутбук
4. Миграция данных через MirrorMaker
Конфигурация репликации:

properties
source.bootstrap.servers = kafka-1:9092,kafka-2:9092,kafka-3:9092
target.bootstrap.servers = kafka-4:8082,kafka-5:8082,kafka-6:8082
topics = shop_products,client_events
replication.factor = 3

5. Spark ETL Pipeline
python
# Логика потоковой обработки
1. Чтение из Kafka (target кластер)
2. Парсинг JSON → DataFrame
3. Агрегация по категориям и брендам
4. Запись в HDFS:
   - Формат: Parquet
   - Партиции: category
   - Checkpoint: HDFS для отказоустойчивости
   
   
Инструкция по запуску
 Запуск инфраструктуры
bash
# Запуск всех сервисов
docker compose up -d

# Проверка статуса
docker compose ps

# Просмотр логов
docker compose logs -f
 Создание топиков и настройка ACL
bash
# Подключение к Kafka
docker exec -it kafka-1 bash

# Создание топиков
kafka-topics --create --topic shop_products \
  --bootstrap-server localhost:9092 --partitions 10 --replication-factor 3

kafka-topics --create --topic client_events \
  --bootstrap-server localhost:9092 --partitions 10 --replication-factor 3

# Настройка ACL
kafka-acls --authorizer-properties zookeeper.connect=zookeeper:2181 \
  --add --allow-principal User:shop-app --operation Write --topic shop_products
 Запуск Python-компонентов
Способ A: Через Docker (рекомендуемый)
bash
# Все Python-сервисы уже запущены в Docker

Проверка работы
bash
# 1. Проверка Kafka топиков
docker exec -it kafka-1 kafka-topics --list --bootstrap-server localhost:9092

# 2. Просмотр сообщений в топике
docker exec -it kafka-1 kafka-console-consumer --topic shop_products \
  --bootstrap-server localhost:9092 --from-beginning


