from pykafka import KafkaClient
from pykafka.common import OffsetType
import websocket
import json

# Kafka connection vars
kafka_host = 'localhost:9092'  
topic = 'bitcoin_transactions' 

# Kafka connection
client = KafkaClient(hosts=kafka_host)
topic = client.topics[topic]
producer = topic.get_producer()

# WebSocket URL
ws_url = 'wss://ws.blockchain.info/inv'

# Handlers for WebSocket
def on_open(ws):
    print('Connected to WebSocket')

def on_message(ws, message):
    try:
        data = json.loads(message)
        producer.produce(bytes(json.dumps(data), 'utf-8'))
    except Exception as e:
        print(f'Error processing message: {e}')

def on_error(ws, error):
    print(f'WebSocket error: {error}')

def on_close(ws):
    print('WebSocket connection closed')

# Initiate websocket connection
ws = websocket.WebSocketApp(ws_url,
                            on_open=on_open,
                            on_message=on_message,
                            on_error=on_error,
                            on_close=on_close)

ws.run_forever()