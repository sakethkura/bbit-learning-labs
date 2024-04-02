import pika
import os
from interfaces.consumerInterface import consumerInterface

class consumerInterface():
    def __init__(self, routing_key : str, **kwargs) -> None:
        self.routing_key = routing_key
        self.setupRMQConnection()
        self.channel = None
    def setupRMQConnection():
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        connection = pika.BlockingConnection(parameters=con_params)
        self.channel = connection.channel()
        self.channel.exchange_declare(exchange="RMQ Labs")

        self.channel.queue_declare(queue="RMQ Labs Queue")
        self.channel.queue_bind(
            queue= "RMQ Labs Queue",
            routing_key= self.routing_key ,
            exchange="RMQ Labs",
        )
        self.channel.basic_consume("RMQ Labs Queue", self.print_output())
    
    def print_output():
        print()

    def startConsuming(self):      
        try:  
            self.channel.start_consuming()
        except KeyboardInterrupt:
            self.channel.close()