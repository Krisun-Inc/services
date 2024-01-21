from awscrt import mqtt, http
from awsiot import mqtt_connection_builder
import threading
import time
import json
import sys
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MqttSubscriber:
    def __init__(self, iot_endpoint, root_ca_path, private_key_path, cert_path, client_id, topic):
        self.iot_endpoint = iot_endpoint
        self.root_ca_path = root_ca_path
        self.private_key_path = private_key_path
        self.cert_path = cert_path
        self.client_id = client_id
        self.topic = topic

    def on_connection_interrupted(self, connection, error, **kwargs):
        logger.error(f"Connection interrupted. Error: {error}")

    def on_connection_resumed(self, connection, return_code, session_present, **kwargs):
        logger.info(f"Connection resumed. Return code: {return_code}, Session present: {session_present}")
        if return_code == mqtt.ConnectReturnCode.ACCEPTED and not session_present:
            logger.info("Session did not persist. Resubscribing to existing topics...")
            resubscribe_future, _ = connection.resubscribe_existing_topics()
            resubscribe_future.add_done_callback(self.on_resubscribe_complete)

    def on_resubscribe_complete(self, resubscribe_future):
        resubscribe_results = resubscribe_future.result()
        logger.info(f"Resubscribe results: {resubscribe_results}")
        for topic, qos in resubscribe_results['topics']:
            if qos is None:
                logger.error(f"Server rejected resubscribe to topic: {topic}")
                sys.exit(1)

    def on_message_received(self, topic, payload, dup, qos, retain, **kwargs):
        logger.info(f"Received message from topic '{topic}': {payload.decode()}")

    def on_connection_success(self, connection, callback_data):
        logger.info(f"Connection Successful with return code: {callback_data.return_code}, Session present: {callback_data.session_present}")

    def on_connection_failure(self, connection, callback_data):
        assert isinstance(callback_data, mqtt.ConnectionFailureData)
        logger.error(f"Connection failed with error code: {callback_data.error}")

    def on_connection_closed(self, connection, callback_data):
        logger.info("Connection closed")

    def run(self):
        mqtt_connection = mqtt_connection_builder.mtls_from_path(
            endpoint=self.iot_endpoint,
            cert_filepath=self.cert_path,
            pri_key_filepath=self.private_key_path,
            ca_filepath=self.root_ca_path,
            on_connection_interrupted=self.on_connection_interrupted,
            on_connection_resumed=self.on_connection_resumed,
            client_id=self.client_id,
            clean_session=False,
            keep_alive_secs=30,
            on_connection_success=self.on_connection_success,
            on_connection_failure=self.on_connection_failure,
            on_connection_closed=self.on_connection_closed
        )

        logger.info(f"Connecting to {self.iot_endpoint} with client ID '{self.client_id}'...")
        connect_future = mqtt_connection.connect()

        connect_future.result()
        logger.info("Connected!")

        logger.info(f"Subscribing to topic '{self.topic}'...")
        subscribe_future, packet_id = mqtt_connection.subscribe(
            topic=self.topic,
            qos=mqtt.QoS.AT_LEAST_ONCE,
            callback=self.on_message_received
        )

        subscribe_result = subscribe_future.result()
        logger.info(f"Subscribed with {subscribe_result['qos']}")

        try:
            while True:
                time.sleep(10)
        except KeyboardInterrupt:
            logger.info("Disconnecting...")
            mqtt_connection.disconnect()

if __name__ == '__main__':
    subscriber = MqttSubscriber(
        iot_endpoint="a2mlk4ov123uc2-ats.iot.us-east-1.amazonaws.com",
        root_ca_path="aws_iot_core_certificates/AmazonRootCA1 (1).pem",
        private_key_path="aws_iot_core_certificates/b6b6d20d162669b8b68c788104f5feafd86ab4a1e517fb5436cc3f2e5f3e3717-private.pem.key",
        cert_path="aws_iot_core_certificates/b6b6d20d162669b8b68c788104f5feafd86ab4a1e517fb5436cc3f2e5f3e3717-certificate.pem.crt",
        client_id="ingest-service",
        topic="Temp_Humi"
    )

    subscriber.run()
