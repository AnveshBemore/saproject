import pika

try:
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    print("✅ Connected to RabbitMQ!")
    connection.close()
except pika.exceptions.AMQPConnectionError as e:
    print("❌ Failed to connect:", e)
