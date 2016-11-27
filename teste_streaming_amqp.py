# UTILIZA PYTHON3
# -*- coding: utf-8 -*-
# Execução:
# spark-submit --jars <arquivo.jar> --py-files <modulo.py> script.py
#
from pyspark import SparkContext, SQLContext
import pika

sc = SparkContext(appName="EventStreamReader")
sqlc = SQLContext(sc)


import pika

#channel.queue_declare(queue='hello')

credentials = pika.PlainCredentials(username='guest', password='guest')
parameters = pika.ConnectionParameters(host='localhost', port=5672)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()
exchange_name = 'amq.topic'

channel.exchange_declare(
    exchange=exchange_name, type='topic', durable=True)

print ' [*] Waiting for messages. To exit press CTRL+C'

def callback(ch, method, properties, body):
    print " [x] Received %r" % (body,)

channel.basic_consume(callback,
                      queue='posts',
                    #   queue='amq.topic',
                    #   exchange=exchange_name,
                    #   routing_key='#',
                      no_ack=True)

print("--AQUI--")

channel.start_consuming()
