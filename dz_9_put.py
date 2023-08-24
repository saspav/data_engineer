"""Генерация потока данных"""

import json
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer

try:
    from faker import Faker

except ImportError:

    class Faker:
        """Заменяет Faker при его недоступности"""

        def __init__(self, *args, **kwargs):
            pass

        @staticmethod
        def last_name():
            return f'user{random.randint(1, 1000):03}'

random.seed(13)
priority_choices = [0] * 15 + [1] * 4 + [2]
random.shuffle(priority_choices)

producer = KafkaProducer(bootstrap_servers='vm-strmng-s-1.test.local:9092')

fake = Faker()
open_date = datetime.now() - timedelta(hours=10)
num_requests = 1000
for idx in range(num_requests):
    open_date += timedelta(seconds=random.randint(0, 60))

    request = {"client": fake.last_name(),
               "opened": open_date.strftime("%d.%m.%Y %H:%M"),
               "priority": priority_choices[idx % 20]}

    producer.send("lab10_pavlov", json.dumps(request).encode('utf-8'))

producer.close()
