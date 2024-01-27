from awscrt import mqtt, http
from awsiot import mqtt_connection_builder
import mysql.connector
import threading
import time
import json
import sys
import logging
import uuid

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database connection parameters
db_config = {
    'host': 'iot-databse.cluster-c9iew80c2mxg.us-east-1.rds.amazonaws.com',
    'port': 3306,
    'user': 'admin',
    'password': 'Vancouver#2020',
    'database': 'iot_data',
}

# Connect to the database
try:
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    logger.info(f"successfully connected to {db_config['database']}")

except mysql.connector.Error as err:
    logger.error(f"Error: {err}")


class MqttSubscriber:
    def __init__(self, iot_endpoint, root_ca_path, private_key_path, cert_path, client_id, topic):
        self.iot_endpoint = iot_endpoint
        self.root_ca_path = root_ca_path
        self.private_key_path = private_key_path
        self.cert_path = cert_path
        self.client_id = client_id
        self.topic = topic
        self.mqtt_connection = None

    def connect_and_subscribe(self):
        self.mqtt_connection = mqtt_connection_builder.mtls_from_path(
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
        connect_future = self.mqtt_connection.connect()
        connect_future.result()

        logger.info(f"Subscribing to topic '{self.topic}'...")
        subscribe_future, _ = self.mqtt_connection.subscribe(
            topic=self.topic,
            qos=mqtt.QoS.AT_LEAST_ONCE,
            callback=self.on_message_received
        )
        subscribe_result = subscribe_future.result()
        logger.info(f"Subscribed with {subscribe_result['qos']}")

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
        try:
            message = json.loads(payload.decode('utf-8'))

            # Extract data from the message
            device_id = str(uuid.uuid4())
            affiliated_org = "KRISUN" #comes from organization device will be purchased from, PROJECT SHOPPING CART
            pubsub_topic = topic #identifies the device name, PROJECT SHOPPING CART
            temperature_c = message.get('Temperature_C', 0.0)
            humidity_percent = message.get('Relative_Humidity', 0.0)
            air_pressure_hpa = message.get('AirPressure_hpa', 0.0)
            co2_ppm = message.get('CO2_ppm', 0)
            voc_index = message.get('VOCIndex', 0)
            pm_1_0 = message.get('PM_1.0', 0)
            pm_2_5 = message.get('PM_2.5', 0)
            pm_10_0 = message.get('PM_10.0', 0)

            # Insert data into the database
            query = (

                "INSERT INTO iot_device_data (Device_ID, Affiliated_Org, PubSub_Topic, "
                "Temperature_C, `Relative_Humidity_%`, AirPressure_hpa, "
                "CO2_ppm, VOCIndex, PM_1_0, PM_2_5, PM_10_0) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

            )
            values = (
                device_id, affiliated_org, pubsub_topic,
                temperature_c, humidity_percent, air_pressure_hpa,
                co2_ppm, voc_index, pm_1_0, pm_2_5, pm_10_0
            )

            cursor.execute(query, values)
            connection.commit()

            logger.info("Data successfully inserted into the database.")
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")

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

class MqttSubscriberFactory:
    @staticmethod
    def create_subscriber(topic):
        return MqttSubscriber(
            iot_endpoint="a2mlk4ov123uc2-ats.iot.us-east-1.amazonaws.com",
            root_ca_path="aws_iot_core_certificates/AmazonRootCA1 (1).pem",
            private_key_path="aws_iot_core_certificates/b6b6d20d162669b8b68c788104f5feafd86ab4a1e517fb5436cc3f2e5f3e3717-private.pem.key",
            cert_path="aws_iot_core_certificates/b6b6d20d162669b8b68c788104f5feafd86ab4a1e517fb5436cc3f2e5f3e3717-certificate.pem.crt",
            client_id=f"ingest-service-{topic}",
            topic=topic
        )


if __name__ == '__main__':
    topics = ["Temp_Humi_1", "Temp_Humi_2", "Temp_Humi_3", "Temp_Humi_4", "Temp_Humi_5"]
    threads = []

    for topic in topics:
        subscriber = MqttSubscriberFactory.create_subscriber(topic)
        thread = threading.Thread(target=subscriber.run)
        threads.append(thread)

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()