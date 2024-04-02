import pika
import os
from producer_interface import mqProducerInterface

class mqProducer(mqProducerInterface):
    def __init__(self, routing_key, exchange_name):
        self.routing_key = routing_key
        self.exchange_name = exchange_name
        self.setupRMQConnection()

    def setupRMQConnection(self):
        conParams = pika.URLParameters(os.environ['AMQP_URL'])
        self.connection = pika.BlockingConnection(parameters=conParams)
        
        self.channel = self.connection.channel()

        self.exchange = self.channel.exchange_declare('Test Exchange')

        # self.channel.queue_declare(queue='hello')

        # self.channel.queue_bind(exchange="Test Exchange", queue='hello', routing_key=self.routing_key)
    
    def publishOrder(self, message):
        self.channel.basic_publish(exchange="Test Exchange", routing_key=self.routing_key, body=message)
        print(f" [x] Sent {self.routing_key}:{message}")

        self.channel.close()
        self.connection.close()





    
    


        