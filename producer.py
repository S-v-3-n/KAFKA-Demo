import json
import uuid
from confluent_kafka import Producer

producer_config = {
    'bootstrap.servers': 'localhost:9092'
}

producer = Producer(producer_config)

order = {
    'order_id': str(uuid.uuid4()),
    'user': 'Lila',
    'item': 'regina_pizza',
    'quantity': 3
}

value = json.dumps(order).encode('utf-8')

def delivery_report(err, msg):
    if err:
        print(f'❌ Delivery failed : {err}')
    else:
        print(f'✅ Delivered {msg.value().decode("utf-8")}')
        print(f'✅ Delivered {msg.topic()} : partition {msg.partition()} : at offset {msg.offset()}')


producer.produce(
    topic='orders',
    value=value,
    callback=delivery_report
)
# Force to send before ending
producer.flush()



