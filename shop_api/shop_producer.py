import json
import os
from confluent_kafka import Producer
import time

# Конфигурация
TOPIC = 'shop_products'

producer_conf = {
        "bootstrap.servers": "kafka-1:29092,kafka-2:29093,kafka-3:29094",  # адрес сервера
        "acks": "2",  #гарантия что сообщение дойдет хотя бы до 2 брокеров
        "retries": 3, #количество повторений отправки при сбое
        "security.protocol": "SSL",
        "ssl.ca.location": "/etc/kafka/secrets/ca.crt",  # Сертификат центра сертификации
        "ssl.certificate.location": "/etc/kafka/secrets/kafka-1-creds/kafka-1.crt",  # Сертификат клиента Kafka
        "ssl.key.location": "/etc/kafka/secrets/kafka-1-creds/kafka-1.key",  # Приватный ключ для клиента Kafka
        "sasl.username": "user",
        "value_serializer": "lambda v: json.dumps(v).encode('utf-8')"
    }


class KafkaProducer:
    def __init__(self, conf, topic):
        self.topic = topic
        self.producer = Producer(conf)

    def delivery_report(self,err, msg):
        if err is not None:
            print(f"Delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def load_products(file_path):
    """Загружает товары из JSON файла"""
    if not os.path.exists(file_path):
        print(f"Файл {file_path} не найден.")
        return []
    with open(file_path, 'r') as f:
        return json.load(f)
    
    print("Запуск Продюсера...")

def send_products(self,producer, products, topic):
    """Отправляет товары в Kafka"""
    for product in products:
        future = producer.produce(topic, key=product.get('product_id'),value=product,callback=self.delivery_report)
        # Блокируем до получения подтверждения (для простоты, в проде так не делают)
        result = future.get(timeout=10)
        print(f"Товар {product.get('product_id')} отправлен в топик {topic}, "
              f"партиция: {result.partition}, оффсет: {result.offset}")
        time.sleep(0.5) # Симуляция потока данных

if __name__ == "__main__":
    prod = KafkaProducer(producer_conf, topic=TOPIC)
    if prod:
        products_list = load_products('data/products.json')
        if products_list:
            send_products(prod, products_list, TOPIC)
        prod.close()