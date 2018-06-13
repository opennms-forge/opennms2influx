# opennms2influx

This consumes collection sets published to Kafka by OpenNMS converts them to InfluxDB's line protocol format
and republishes them back to a different Kafka topic.

These can then be consumed by https://github.com/mre/kafka-influxdb and inserted into Influx.


