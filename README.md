# smarthome-influxdb
Yet another InfluxDB plugin for Smarthome.py

## Why?
There are already few implementations out there. For example: [rthill/influxdata](https://github.com/rthill/influxdata) and [SgtSeppel/influxdb](https://github.com/SgtSeppel/influxdb)
This implementation actually predates both as I initially implemented it for InfluxDB 0.8. Since then I ported it to InfluxDB >0.9.

Features
* Uses InfluxDB python client and http
* Buffers writes and retries them in case of an error (network errors for instance)
* Supports Smarthome.py's series api to expose series to Smatvisu

## Installation
Install following dependencies either with pip or use your OS packages
* influxdb
* pytz
* pyrfc3339

## Configuration (plugin.conf)
Example:
```
[influxdb]
    class_name = InfluxDB
    class_path = plugins.influxdb
    host = localhost
    port = 8086
    user = smarthome
    passwd = smarthome
    database = metrics
    write_queue_max_size = 1000
    cycle = 300

```
## Usage
Put `influxdb = true` or `influxdb = init` to your item configuration.

