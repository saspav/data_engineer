import json
import pg8000
from datetime import datetime
from configparser import ConfigParser
from kafka import KafkaConsumer

config = ConfigParser()
config.read('greenplum.ini')
base_name = 'GreenPlum'
dbname = config.get(base_name, 'databases')
user = config.get(base_name, 'user_name')
pswd = config.get(base_name, 'user_pass')
host = config.get(base_name, 'host_urls')
port = config.get(base_name, 'host_port')

try:
    conn = pg8000.connect(database=dbname, user=user, password=pswd, host=host, port=port)

    consumer = KafkaConsumer(
        "lab10_pavlov",
        bootstrap_servers='vm-strmng-s-1.test.local:9092',
        group_id='my-group',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    for message in consumer:
        data = message.value
        client = data.get('client', 'none_client')
        opened = data.get('opened', '01.01.2000 00:00')
        opened = datetime.strptime(opened, '%d.%m.%Y %H:%M').strftime('%Y-%m-%d %H:%M')

        table_name = f"lab10_pavlov_{data.get('priority', 0)}"

        query = "INSERT INTO {} (client, opened) VALUES (%s, %s)".format(table_name)
        cursor = conn.cursor()
        cursor.execute(query, (client, opened))
        conn.commit()

        print(query, (client, opened))

finally:
    consumer.close()
    conn.close()
