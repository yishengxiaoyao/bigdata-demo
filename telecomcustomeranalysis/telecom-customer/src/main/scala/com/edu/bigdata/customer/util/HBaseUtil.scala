package com.edu.bigdata.customer.util

import java.text.DecimalFormat
import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, NamespaceDescriptor, TableName}
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.util.Bytes

object HBaseUtil {

  /**
    * 判断 HBase 表是否存在（使用新 HBase 的 API）
    * 小知识：当前代码块对该异常没有处理能力(业务处理能力)的时候，我们就要抛出去。
    *
    * @param conf      HBaseConfiguration
    * @param tableName
    * @return
    */
  def isExistTable(conf:Configuration,tableName:String): Boolean ={
    val conn = ConnectionFactory.createConnection(conf)
    val admin = conn.getAdmin()
    val result = admin.tableExists(TableName.valueOf(tableName))
    admin.close()
    conn.close()
    result
  }

  /**
    * 初始化命名空间
    *
    * @param conf
    * @param namespace
    */
  def initNamespace(conf:Configuration,namespace:String): Unit ={
    val conn = ConnectionFactory.createConnection(conf)
    val admin = conn.getAdmin()
    // 命名空间类似于关系型数据库中的 schema，可以想象成文件夹
    // 创建命名空间描述器
    val nd = NamespaceDescriptor.create(namespace)
            .addConfiguration("CREATE_TIME",String.valueOf(System.currentTimeMillis()))
            .addConfiguration("AUTHOR","xiaoyao")
            .build()
    admin.createNamespace(nd)
    admin.clone()
    conn.close()
  }
  /**
    * 创建表+预分区键
    *
    * @param conf
    * @param tableName
    * @param regions
    * @param columnFamily
    */

  def createTable(conf:Configuration,tableName: String,regions:Int,columnFamily:String*): Unit ={
    val conn = ConnectionFactory.createConnection(conf)
    val admin = conn.getAdmin()
    if (!isExistTable(conf,tableName)){
      // 创建表描述器（即通过表名实例化表描述器）
      val htd = new HTableDescriptor(TableName.valueOf(tableName))
      // 添加列族
      for (cf<- columnFamily){
        // 创建列族描述器
        val hcd = new HColumnDescriptor(cf)
        // 可以设置保存的版本个数，默认是1个
        // hcd.setMaxVersions(3);
        htd.addFamily(hcd)
      }
      // 创建表操作（简单表）
      // admin.createTable(htd);

      // 为该表设置协处理器
      // htd.addCoprocessor("com.china.hbase.CalleeWriteObserver");

      // 创建表操作（加预分区）
      admin.createTable(htd,genSplitKeys(regions))
      admin.clone()
      conn.close()
    }
  }

  /**
    * 生成预分区键
    * 例如：{"00|", "01|", "02|", "03|", "04|", "05|"}
    *
    * @param regions
    * @return
    */
  def genSplitKeys(regions: Int): Array[Array[Byte]] ={
    val df = new DecimalFormat("00")
    val keys = new Array[String](regions)
    for (i <- 1 to regions){
      keys(i) = df.format(i-1) + "|"
    }
    val treeSet = new util.TreeSet[Array[Byte]](Bytes.BYTES_COMPARATOR)
    for (i<- 1 to regions){
      treeSet.add(Bytes.toBytes(keys(i-1)))
    }
    val splitKeys:Array[Array[Byte]] = Array.ofDim[Byte](regions,1)
    val splitKeysIterator = treeSet.iterator()

    var index:Int = 0
    while (splitKeysIterator.hasNext){
      val b:Array[Byte] = splitKeysIterator.next()
      splitKeys(index) = b
      index = index + 1
    }
    splitKeys
  }

  /**
    * 生成 RowKey
    * 形式为：regionCode_call1_buildTime_call2_flag_duration
    *
    * @param regionCode
    * @param call1
    * @param buildTime
    * @param call2
    * @param flag
    * @param duration
    * @return
    */

  def genRowKey(regionCode:String,call1:String,buildTime:String,call2:String,
                flag:String,duration:String): String ={
    regionCode+"_"+call1+"_"+buildTime+"_"+call2+"_"+flag+"_"+duration
  }

  /**
    * 生成分区号
    * 手机号：15837312345
    * 通话建立的时间：2017-01-10 11:20:30 -> 201701
    *
    * @param call1
    * @param buildTime
    * @param regions
    * @return
    */

  def genRegionCode(call1:String,buildTime:String,regions:Int):String={
    val len:Int = call1.length()
    val lastPhone:String = call1.substring(len-4)
    val ym:String = buildTime.replaceAll("-","").substring(0,6)
    val x:Int = lastPhone.toInt ^ ym.toInt
    val y:Int = x.hashCode()
    val regionCode = y % regions
    val df = new DecimalFormat("00")
    df.format(regionCode)
  }


}
