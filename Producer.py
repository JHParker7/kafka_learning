from kafka import KafkaProducer
from datetime import datetime
import json
producer = KafkaProducer(
    bootstrap_servers=['localhost:9093'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
for i in range(100):
    producer.send('posts', i)
for i in range(100):
    producer.send('posts', {'author': 'ME', 'content': 'Kafka is easy!', 'created_at': datetime.now().isoformat(),"index":i})