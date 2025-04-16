import pika, json

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='payment-applied')

def callback(ch, method, properties, body):
    order = json.loads(body)
    order_id = order['order_id']
    student_name = order['student_name']
    print(f"[{student_name}] Fulfillment: Order {order_id} fulfilled. Events published.")

    channel.basic_publish(exchange='', routing_key='order-fulfilled', body=json.dumps(order))
    channel.basic_publish(exchange='', routing_key='notification', body=json.dumps({'event_type': 'order-fulfilled', **order}))

channel.basic_consume(queue='payment-applied', on_message_callback=callback, auto_ack=True)
print("Waiting for payment-applied events...")
channel.start_consuming()
