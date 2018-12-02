
from kafka import KafkaProducer


bootstrap_servers=['localhost:9092']
topicName='newtopic1'

producer=KafkaProducer(bootstrap_servers = bootstrap_servers)
i=0
while (i<100):
 ack=producer.send(topicName,b'ABC'+str(i))
 metadata = ack.get()
 print(metadata.topic)
 print(metadata.partition)
 i=i+1




