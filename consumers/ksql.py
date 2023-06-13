"""Configures KSQL to combine station and turnstile data"""
import json
import logging
import requests
import topic_check


logger = logging.getLogger(__name__)


KSQL_URL = "http://localhost:8088"

KSQL_STATEMENT = """
CREATE TABLE turnstile_table_ksql (
    station_id INTEGER,
    station_name VARCHAR,
    line VARCHAR,
    num_entries INTEGER
) WITH (
    KAFKA_TOPIC='turnstile',
    VALUE_FORMAT='AVRO',
    KEY='station_id'
);

CREATE TABLE turnstile_summary
    WITH (VALUE_FORMAT='JSON') AS
        SELECT station_id,station_name,COUNT(station_id) AS COUNT 
        FROM turnstile_table_ksql 
        GROUP BY (station_id,station_name);
"""


def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    if topic_check.topic_exists("TURNSTILE_SUMMARY") is True:
        print("exists")
        return

    print("executing ksql statement...")

    resp = requests.post(
        f"{KSQL_URL}/ksql",
        headers={"Content-Type": "application/vnd.ksql.v1+json"},
        data=json.dumps(
            {
                "ksql": KSQL_STATEMENT,
                "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"},
            }
        ),
    )

    # Ensure that a 2XX status code was returned
    try:
        resp.raise_for_status()
    except Exception as e:
        print(e)


if __name__ == "__main__":
    execute_statement()
