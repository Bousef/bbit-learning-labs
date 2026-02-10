import pika
import os

from producer_interface import mqProducerInterface

class mqProducer(mqProducerInterface):

    def __init__(self, routing_key, exchange_name):
        self.routing_key = routing_key
        self.exchange_name = exchange_name
        self.channel = None
        self.connection = None
    def setupRMQConnection(self) -> None:
        # Set-up Connection to RabbitMQ service
        conParams = pika.URLParameters(os.environ['AMQP_URL'])
        self.connection = pika.BlockingConnection(parameters=conParams)
        print(self.connection)
        # Establish Channel
        self.channel = self.connection.channel()
        print(self.channel)
        # Create the exchange if not already present
        self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='direct')
        

    def publishOrder(self, message: str) -> None:
        self.setupRMQConnection()
        # Basic Publish to Exchange
        self.channel.basic_publish(exchange=self.exchange_name, routing_key=self.routing_key, body=message)
        # Close Channel
        self.channel.close()
        # Close Connection
        
        self.connection.close()