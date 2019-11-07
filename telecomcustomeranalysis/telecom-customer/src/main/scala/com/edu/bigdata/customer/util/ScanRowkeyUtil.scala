package com.edu.bigdata.customer.util

import java.util.{Calendar, Date}

import org.apache.commons.lang3.time.FastDateFormat

import scala.collection.mutable.ListBuffer

class ScanRowkeyUtil extends Serializable {

  var telephone:String = _
  var startDateString:String = _
  var stopDateString:String = _
  var list:ListBuffer[Array[String]] = _
  var index:Int = 0

  val df1 = new FastDateFormat("yyyy-MM-dd")
  val df2 = new FastDateFormat("yyyyMMddHHmmss")


  this(telephone:String , startDateString:String , stopDateString:String ) {
    this.telephone = telephone;
    this.startDateString = startDateString;
    this.stopDateString = stopDateString;

    list = new ListBuffer[Array[String]]()
    genRowKeys()
  }

  // 01_15837312345_201711
  // 15837312345 2017-01-01 2017-05-01
  def genRowKeys():Unit={
    val regions:Int = PropertiesUtil.getProperty("hbase.calllog.regions.count").toInt
    val startDate = df1.parse(startDateString)
    val stopDate = df2.parse(stopDateString)
    val currentStartCalendar = Calendar.getInstance()
    currentStartCalendar.setTimeInMillis(startDate.getTime())
    val currentStopCalendar = Calendar.getInstance()
    currentStopCalendar.setTimeInMillis(stopDate.getTime())
    currentStopCalendar.add(Calendar.MONTH,1)

    while (stopDate.getTime() >= currentStopCalendar.getTimeInMillis){
      val regionCode = HBaseUtil.genRegionCode(telephone,df2.format(new Date(currentStartCalendar.getTimeInMillis())),regions)
      val startRowKey:String = regionCode + "_" + telephone + "_" + df2.format(new Date(currentStartCalendar.getTimeInMillis()));
      val stopRowKey:String = regionCode + "_" + telephone + "_" + df2.format(new Date(currentStopCalendar.getTimeInMillis()));

      val rowkeys:Array[String] = Array(startRowKey, stopRowKey);
      list.append(rowkeys);
      currentStartCalendar.add(Calendar.MONTH, 1);
      currentStopCalendar.add(Calendar.MONTH, 1);
    }

  }

  /**
    * 判断 list 集合中是否还有下一组 rowKey
    *
    * @return
    */

  def hasNext(): Boolean ={
    if (index < list.size){
      true
    }else{
      false
    }
  }

  /**
    * 取出 list 集合中存放的下一组 rowKey
    *
    * @return
    */

  def next():Array[String]={
    val rowkeys = list(index)
    index = index + 1
    rowkeys
  }

}
