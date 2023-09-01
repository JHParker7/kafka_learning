from kafka import KafkaConsumer
import json
import pandas as pd
consumer = KafkaConsumer(
    'data',
    bootstrap_servers=['localhost:9093'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)
# note that this for loop will block forever to wait for the next message
print(consumer.poll(100))
x=0
for message in consumer:
    x=x+1
    print(x)
    print(pd.read_json(message.value))