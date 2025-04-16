import pika, json

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='order-fulfilled')

def callback(ch, method, properties, body):
    order = json.loads(body)
    order_id = order['order_id']
    student_name = order['student_name']
    print(f"[{student_name}] Shipping: Order {order_id} shipped. Events published.")

    channel.basic_publish(exchange='', routing_key='order-shipped', body=json.dumps(order))
    channel.basic_publish(exchange='', routing_key='notification', body=json.dumps({'event_type': 'order-shipped', **order}))

channel.basic_consume(queue='order-fulfilled', on_message_callback=callback, auto_ack=True)
print("Waiting for order-fulfilled events...")
channel.start_consuming()
