from kafka import KafkaProducer
from datetime import datetime
import json
import pandas as pd
df=pd.DataFrame([[1,2,3,4,5,6,7,8,9,10],[1,2,3,4,5,6,7,8,9,10],[1,2,3,4,5,6,7,8,9,10],[1,2,3,4,5,6,7,8,9,10],[1,2,3,4,5,6,7,8,9,10]])
producer = KafkaProducer(
    bootstrap_servers=['localhost:9093'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
for i in range(10000):
    producer.send('posts', {'author': 'ME', 'content': 'Kafka is easy! number: '+str(i), 'created_at': datetime.now().isoformat(),"index":i})
producer.send('data',df.to_json())