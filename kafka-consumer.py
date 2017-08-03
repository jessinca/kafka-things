import sys
import signal
import os
from pykafka.common import OffsetType
from pykafka import KafkaClient, SslConfig
from distutils.util import strtobool


def signal_handler(signal, frame):
    print 'Consumed %s messages' % message_count
    sys.exit(0)


if len(sys.argv) < 3:
  print 'Usage : python kafka-consumer.py <broker> <topic>'
  sys.exit(1)

brokers = sys.argv[1]
topic = sys.argv[2]
is_using_latest_offset = strtobool(sys.argv[3]) if len(sys.argv) > 3 else 0

cafile = os.getenv('KAFKA_CA_FILE')
certfile = os.getenv('KAFKA_CERT_FILE')
keyfile = os.getenv('KAFKA_PRIVATE_KEY')
message_count = 0

if cafile is not None and certfile is not None and keyfile is not None:
  config = SslConfig(cafile=cafile,
                    certfile=certfile, 
                    keyfile=keyfile)
  client = KafkaClient(hosts=brokers, ssl_config=config)
else:
  client = KafkaClient(hosts=brokers)

topic = client.topics[topic]

signal.signal(signal.SIGINT, signal_handler)

auto_offset_reset = OffsetType.LATEST if is_using_latest_offset else OffsetType.EARLIEST
consumer = topic.get_simple_consumer(
    auto_offset_reset=auto_offset_reset
)
for message in consumer:
  if message is not None:
    print message.offset, message.value
    message_count += 1
