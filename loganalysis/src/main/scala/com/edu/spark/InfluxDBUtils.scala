package com.edu.spark


object InfluxDBUtils {
  /**
    * 获取influxdb数据库的ip
    *
    * @return
    */
  def getInfluxIP: String = {
    var ip = "192.168.0.77"
    val getenv = System.getenv
    if (getenv.containsKey("INFLUXDB_IP")) ip = getenv.get("INFLUXDB_IP")
    ip
  }

  /**
    * 获取随机度量
    *
    * @return
    */
  def getRandomMeasurement: String = "measurement_" + System.nanoTime

  /**
    * 获取influxdb数据库的端口
    *
    * @param apiPort
    * @return
    */
  def getInfluxPORT(apiPort: Boolean): String = {
    var port = "8086"
    val getenv = System.getenv
    if (apiPort) if (getenv.containsKey("INFLUXDB_PORT_API")) port = getenv.get("INFLUXDB_PORT_API")
    else {
      port = "8096"
      if (getenv.containsKey("INFLUXDB_PORT_COLLECTD")) port = getenv.get("INFLUXDB_PORT_COLLECTD")
    }
    port
  }

  /**
    * 获取保留策略
    *
    * @param version
    * @return
    */
  def defaultRetentionPolicy(version: String): String = if (version.startsWith("0.")) "default"
  else "autogen"
}
