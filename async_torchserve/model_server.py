import json
import logging
from kafka.admin import KafkaAdminClient, NewTopic
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from image_classification.utils import import_class
from kafka.errors import TopicAlreadyExistsError

log = logging.getLogger(__name__)


class ModelServer:

    def __init__(self, model_config, loop, bootstrap_servers):
        model_class = import_class(
            model_config["module_name"], model_config["class_name"]
        )
        self.model = model_class()
        log.info(f"Initialized model server for {self.model.name}")

        base_topic_name = ".".join([
            "model_server",
            self.model.name,
            str(self.model.major_version),
            str(self.model.minor_version),
        ])
        self.consumer_topic = f"{base_topic_name}.inputs"
        self.producer_topic = f"{base_topic_name}.outputs"
        log.info(f"{self.model.name}: consuming messages from topic {self.consumer_topic}")
        log.info(f"{self.model.name}: producing messages to topic {self.producer_topic}")
        topics = [
            NewTopic(self.consumer_topic, num_partitions=1, replication_factor=1),
            NewTopic(self.producer_topic, num_partitions=1, replication_factor=1)
        ]
        admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        try:
            admin_client.create_topics(topics, validate_only=False)
        except TopicAlreadyExistsError as e:
            log.info(f"Topics already exist. Continuing.")
        self.consumer = AIOKafkaConsumer(
            self.consumer_topic, loop=loop, 
            bootstrap_servers=bootstrap_servers, group_id=__name__
        )
        self.producer = AIOKafkaProducer(
            loop=loop, bootstrap_servers=bootstrap_servers
        )
    
    async def start(self):
        log.info(f"{self.model.name}: starting producer and consumer")
        await self.consumer.start()
        await self.producer.start()

    async def process(self):
        async for message in self.consumer:
            try:
                data = json.loads(message.value)
                prediction = self.model(data)
                serialized_prediction = json.dumps(prediction).encode()
                await self.producer.send_and_wait(
                    self.producer_topic, serialized_prediction
                )
            except Exception as e:
                log.error(f"{self.model.name} exception: {e}")
    
    async def stop(self):
        log.info(f"{self.model.name}: stopping producer and consumer")
        await self.consumer.stop()
        await self.producer.stop()
