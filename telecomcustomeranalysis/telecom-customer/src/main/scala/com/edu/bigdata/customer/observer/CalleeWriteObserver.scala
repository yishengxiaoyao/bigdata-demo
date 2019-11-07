package com.edu.bigdata.customer.observer

import com.edu.bigdata.customer.util.{HBaseUtil, PropertiesUtil}
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Durability, Put}
import org.apache.hadoop.hbase.coprocessor.{BaseRegionObserver, ObserverContext, RegionCoprocessorEnvironment}
import org.apache.hadoop.hbase.regionserver.wal.WALEdit
import org.apache.hadoop.hbase.util.Bytes

object CalleeWriteObserver extends BaseRegionObserver{
  override def postPut(e: ObserverContext[RegionCoprocessorEnvironment], put: Put, edit: WALEdit, durability: Durability): Unit = {
    super.postPut(e, put, edit, durability)
    // 1、获取你想要操作的目标表的名称
    val targetTableName = PropertiesUtil.getProperty("hbase.calllog.tableName")
    // 2、获取当前操作的表的表名
    val currentTableName = e.getEnvironment().getRegion().getRegionInfo().getTable().getNameAsString()

    // 3、判断需要操作的表是否就是当前表，如果不是，则直接 return
    if (targetTableName.equals(currentTableName)) {
      // 4、得到当前插入数据的值并封装新的数据，
      // oriRowkey举例：01_15369468720_20170727081033_13720860202_1_0180
      val oriRowkey = Bytes.toString(put.getRow)

      val splits = oriRowkey.split("_")
      // 如果当前插入的是被叫数据，则直接返回(因为默认提供的数据全部为主叫数据)，
      // 又因为我们使用的协处理器的方法是postPut()，即每次插入一条主叫数据后，都会调用该方法插入一条被叫数据。
      // 插入一条被叫数据后，又会继续调用该方法，此时插入的数据是被叫数据，需要及时停掉，否则会形成死循环！
      val oldFlag = splits(4);
      if (!oldFlag.equals("0")){
        val regions:Int = PropertiesUtil.getProperty("hbase.calllog.regions.count").toInt
        val call1:String = splits(1)
        val call2:String = splits(3)
        val buildTime:String = splits(2)
        val duration:String = splits(5)
        val newFlag = "0"
        val buildTime_ts = (new FastDateFormat("yyyyMMddHHmmss")).parse(buildTime).getTime().toString()
        val regionCode = HBaseUtil.genRegionCode(call2,buildTime,regions)
        val rowKey = HBaseUtil.genRowKey(regionCode,call2,buildTime,call1,newFlag,duration)
        val calleePut = new Put(Bytes.toBytes(rowKey))
        calleePut.addColumn(Bytes.toBytes("f2"),Bytes.toBytes("call1"),Bytes.toBytes(call2))
        calleePut.addColumn(Bytes.toBytes("f2"),Bytes.toBytes("call2"),Bytes.toBytes(call1))
        calleePut.addColumn(Bytes.toBytes("f2"),Bytes.toBytes("build_time"),Bytes.toBytes(buildTime))
        calleePut.addColumn(Bytes.toBytes("f2"),Bytes.toBytes("build_time_ts"),Bytes.toBytes(buildTime_ts))
        calleePut.addColumn(Bytes.toBytes("f2"),Bytes.toBytes("flag"),Bytes.toBytes(newFlag))
        calleePut.addColumn(Bytes.toBytes("f2"),Bytes.toBytes("duration"),Bytes.toBytes(duration))
        val table = e.getEnvironment().getTable(TableName.valueOf(targetTableName))
        table.put(calleePut)
        table.close()
      }
    }
  }
}
