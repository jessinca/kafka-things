import sys
import os
from pykafka import KafkaClient, SslConfig


if len(sys.argv) < 3:
  print 'Usage : python kafka-consumer.py <broker> <topic>'
  sys.exit(1)

brokers = sys.argv[1]
topic = sys.argv[2]
cafile = os.getenv('KAFKA_CA_FILE')
certfile = os.getenv('KAFKA_CERT_FILE')
keyfile = os.getenv('KAFKA_PRIVATE_KEY')

if cafile is not None and certfile is not None and keyfile is not None:
  config = SslConfig(cafile=cafile,
                    certfile=certfile, 
                    keyfile=keyfile)
  client = KafkaClient(hosts=brokers, ssl_config=config)
else:
  client = KafkaClient(hosts=brokers)

topic = client.topics[topic]

consumer = topic.get_simple_consumer()
for message in consumer:
  if message is not None:
    print message.offset, message.value
