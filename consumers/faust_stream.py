"""Defines trends calculations for stations"""
import logging
import faust
logger = logging.getLogger(__name__)

# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str

app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
topic = app.topic("project1_stations", value_type=Station)
out_topic = app.topic("station_output",value_type=TransformedStation, partitions=1)

table = app.Table(
   "station_table",
   default=TransformedStation,
   partitions=1,
   changelog_topic=out_topic,
)


@app.agent(topic)
async def transformLine(stations):
    print("init transformLine")
    
    async for station in stations:
        line = ""
        if station.red:
            line = "red"
        elif station.blue:
            line='blue'
        else:
            line='green'
        await out_topic.send( value=TransformedStation(
            station_id= station.station_id,
            station_name=station.station_name,
            order=station.order,
            line=line
        ), key=str(station.station_id))

@app.agent(out_topic)
async def updateTable(transformeds):
    print("writing to table...")
    async for station in transformeds:
        print(f"---{station}")
        table[station.station_id] = station

if __name__ == "__main__":
    app.main()
