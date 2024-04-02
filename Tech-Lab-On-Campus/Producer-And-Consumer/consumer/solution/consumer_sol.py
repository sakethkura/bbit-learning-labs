import pika
import os
from consumer_interface import mqConsumerInterface

class mqConsumer(mqConsumerInterface):
    def __init__(self, binding_key: str, exchange_name: str, queue_name: str) -> None:
        self.binding_key = binding_key
        self.exchange_name = exchange_name
        self.queue_name = queue_name
        self.setupRMQConnection()
        self.channel = None
        self.connection = None
    def setupRMQConnection(self):
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        self.connection = pika.BlockingConnection(parameters=con_params)
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange= self.exchange_name)

        self.channel.queue_declare(queue=self.queue_name )
        self.channel.queue_bind(
            queue= self.queue_name ,
            routing_key= self.binding_key,
            exchange=self.exchange_name,
        )
        self.channel.basic_consume(self.queue_name ,self.on_message_callback, auto_ack=False)
        self.startConsuming()
    
    def __del__(self) -> None:
        print("Closing RMQ connection on destruction")
        self.channel.close()
        self.connection.close()
    
    def on_message_callback(self, channel, method_frame, header_frame, body) -> None:
        print(method_frame,header_frame, body)

    def startConsuming(self):      
        try:  
            self.channel.start_consuming()
        except KeyboardInterrupt:
            self.channel.stop_consuming()