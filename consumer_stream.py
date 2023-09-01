from kafka import KafkaConsumer
import json
import pandas as pd
import time
consumer = KafkaConsumer(
    'posts',
    bootstrap_servers=['localhost:9093'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)
# note that this for loop will block forever to wait for the next message
x=0
for message in consumer:
    x=x+1
 #   print(x)
    val=message.value
    val=pd.Series(val)
  #  print("")
 #   print(val)
 #   val.to_frame().T.set_index("index",drop=True).to_csv("test_1.csv")
    new_data=val.to_frame().T.set_index("index",drop=True)
    new_data["created_at"]=pd.to_datetime(new_data["created_at"])
    if x == 1: 
        start=time.time()
        new_data.to_csv("test_1.csv")
        pass
    else:
      df=pd.read_csv("test_1.csv").set_index("index",drop=True)
      df=pd.concat([df,new_data])
      df["created_at"]=pd.to_datetime(df["created_at"])
      df=df.sort_values('created_at').drop_duplicates(["author","content"],keep='last')
      df.to_csv("test_1.csv")
    print(x)
    if x==10000: break
end=time.time()
print(start)
print(end)
print(end-start)