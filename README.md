# Kafka-consumer

Simple consumer using Pykafka https://github.com/Parsely/pykafka.

## Usage

```bash
pip install -f requirements.txt

export KAFKA_CA_FILE=<cafile>
export KAFKA_CERT_FILE=<certfile>
export KAFKA_PRIVATE_KEY=<privatekey>
python kafka-consumer.py <broker> <topic>
```
