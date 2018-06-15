# opennms2influx

This consumes collection sets published to Kafka by OpenNMS and inserts them into InfluxDB using the Java API.



## Getting Started

Fire up InfluxDB using Docker

```
mkdir /tmp/influx
docker run -p 8086:8086 -v /tmp/influx:/var/lib/influxdb influxdb
```
