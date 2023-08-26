import json
from datetime import datetime
import pg8000



class KafkaProducer:
    def __init__(self, *args, **kwargs):
        pass

    @staticmethod
    def send(query, data):
        with open(query, 'ab') as file:
            file.write(data + b'\n')

    def close(self):
        pass


class DummyMsg:
    def __init__(self, text):
        self.text = text

    @property
    def value(self):
        return self.text


class KafkaConsumer:
    def __init__(self, query, value_deserializer=lambda x: x, **kwargs):
        self.query = query
        self.rows = []
        self.func = value_deserializer

    def close(self):
        pass

    def __iter__(self):
        with open(self.query, 'rb') as file:
            self.rows = file.readlines()
            try:
                return map(lambda s: DummyMsg(self.func(s)), self.rows)
            except:
                return map(lambda s: DummyMsg(s), self.rows)


if __name__ == "__main__":
    name_lab = "lab10_test"
    request = {
        "client": "Ivanov",
        "opened": "21.08.2023 13:00",
        "priority": 2
    }
    producer = KafkaProducer(bootstrap_servers='vm-strmng-s-1.test.local:9092')
    producer.send(name_lab, json.dumps(request).encode('utf-8'))

    dbname = 'databases'
    user = 'user_name'
    pswd = 'user_pass'
    host = 'host_urls'
    port = 'host_port'
    conn = pg8000.connect(database=dbname, user=user, password=pswd, host=host, port=port)
    cursor = conn.cursor()

    consumer = KafkaConsumer(
        name_lab,
        bootstrap_servers='vm-strmng-s-1.test.local:9092',
        group_id='my-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    for message in consumer:
        data = message.value
        client = data.get('client', 'none_client')
        opened = data.get('opened', '01.01.2000 00:00')
        opened = datetime.strptime(opened, '%d.%m.%Y %H:%M').strftime('%Y-%m-%d %H:%M')

        table_name = f"{name_lab}_{data.get('priority', 0)}"

        query = "INSERT INTO {} (client, opened) VALUES (%s, %s)".format(table_name)
        cursor.execute(query, (client, opened))
        conn.commit()

        print(query, (client, opened))
