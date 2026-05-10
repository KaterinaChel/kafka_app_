import json
from kafka import KafkaProducer
import uuid
from datetime import datetime


TOPIC_EVENTS = 'client_events'

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

producer = KafkaProducer(
    bootstrap_servers="kafka-1:29092,kafka-2:29093,kafka-3:29094",
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def send_event(self,event_type, query,topic):
    """Отправляет событие клиента в Kafka"""
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type, # 'search' или 'recommendation_request'
        "user_id": "user_001", # Фиксированный для демо
        "query": query,
        "timestamp": datetime.now().isoformat()
    }
    producer.produce(topic, key=event.get('event_id'),value=event,callback=self.delivery_report)
    print(f"Отправлено событие: {event}")
    producer.flush()

if __name__ == "__main__":
    print("Введите команду: search <имя_товара> или recs")
    prod = KafkaProducer(producer_conf, topic=TOPIC_EVENTS)
    while True:
        try:
            user_input = input("> ").strip()
            if not user_input:
                continue
            parts = user_input.split()
            cmd = parts[0]
            if cmd == 'search' and len(parts) > 1:
                send_event('SEARCH', ' '.join(parts[1:]),TOPIC_EVENTS)
            elif cmd == 'recs':
                send_event('GET_RECOMMENDATIONS', '')
            else:
                print("Неизвестная команда.")
        except KeyboardInterrupt:
            break
    producer.close()