"""Configures a Kafka Connector for Postgres Station data"""
import json
import logging
import requests

logger = logging.getLogger(__name__)

KAFKA_CONNECT_URL = "http://localhost:8083/connectors"
CONNECTOR_NAME = "stations"

def configure_connector():
    """Starts and configures the Kafka Connect connector"""
    print("creating or updating kafka connect connector...")

    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        print("connector already created skipping recreation")
        return
    
    resp = requests.post(
       KAFKA_CONNECT_URL,
       headers={"Content-Type": "application/json"},
       data=json.dumps({
           "name": CONNECTOR_NAME,
           "config": {
               "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
               "key.converter": "org.apache.kafka.connect.json.JsonConverter",
               "key.converter.schemas.enable": "false",
               "value.converter": "org.apache.kafka.connect.json.JsonConverter",
               "value.converter.schemas.enable": "false",
               "batch.max.rows": "500",
               "connection.url": "jdbc:postgresql://postgres:5432/cta",
               "connection.user": "cta_admin",
               "connection.password": "chicago",
               "table.whitelist": "stations",
               "mode": "incrementing",
               "incrementing.column.name": "stop_id",
               "topic.prefix": f"{CONNECTOR_NAME}_",
               "poll.interval.ms": "3600000",
           }
       }),
    )

    try:
        resp.raise_for_status()
        print("Post connectorResponse initial:", resp.content)
    except requests.exceptions.HTTPError as err:
        print("Post connectorResponse except:", resp.content)
        print(f"HTTP error occurred: {err}")
    else:
        print("Post connectorResponse else:", resp.content)


if __name__ == "__main__":
    configure_connector()
