package com.edu.spark

/**
  * 定义一个类来接收数据
  *
  * @param hostName      主机的名称
  * @param serviceName   服务的名字
  * @param lineTimestamp 时间戳
  * @param logType       log的类型
  * @param logInfo       log信息
  */
case class CDHRoleLog(hostName: String, serviceName: String, lineTimestamp: String, logType: String, logInfo: String)
