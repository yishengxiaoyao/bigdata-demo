package com.edu.bigdata.customer.util

import java.util

import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.util.Bytes

object HBaseFilterUtil {
  /**
    * 获得相等过滤器。相当于SQL的 [字段] = [值]
    *
    * @param cf    列族名
    * @param col   列名
    * @param val   值
    * @return      过滤器
    */
  def eqFilter(cf:String,col:String,value:Array[Byte]):Filter={
    val f = new SingleColumnValueFilter(Bytes.toBytes(cf),Bytes.toBytes(col),CompareFilter.CompareOp.EQUAL,value)
    f.setLatestVersionOnly(true)
    f.setFilterIfMissing(true)
    f
  }

  /**
    * 获得大于过滤器。相当于SQL的 [字段] > [值]
    *
    * @param cf    列族名
    * @param col   列名
    * @param val   值
    * @return      过滤器
    */

  def gtFilter(cf:String,col:String,value:Array[Byte]): Filter ={
    val f = new SingleColumnValueFilter(Bytes.toBytes(cf),Bytes.toBytes(col),CompareFilter.CompareOp.GREATER,value)
    f.setLatestVersionOnly(true)
    f.setFilterIfMissing(true)
    f
  }

  /**
    * 获得大于等于过滤器。相当于SQL的 [字段] >= [值]
    *
    * @param cf    列族名
    * @param col   列名
    * @param val   值
    * @return      过滤器
    */
  def gteqFilter(cf: String, col: String, value: Array[Byte]): Filter ={
    val f = new SingleColumnValueFilter(Bytes.toBytes(cf),Bytes.toBytes(col),CompareFilter.CompareOp.GREATER_OR_EQUAL,value)
    f.setLatestVersionOnly(true)
    f.setFilterIfMissing(true)
    f
  }

  /**
    * 获得小于过滤器。相当于SQL的 [字段] < [值]
    *
    * @param cf    列族名
    * @param col   列名
    * @param val   值
    * @return      过滤器
    */

  def ltFilter(cf:String,col:String,value:Array[Byte]):Filter={
    val f = new SingleColumnValueFilter(Bytes.toBytes(cf),Bytes.toBytes(col),CompareFilter.CompareOp.LESS,value)
    f.setLatestVersionOnly(true)
    f.setFilterIfMissing(true)
    f
  }

  /**
    * 获得小于等于过滤器。相当于SQL的 [字段] <= [值]
    *
    * @param cf    列族名
    * @param col   列名
    * @param val   值
    * @return      过滤器
    */

  def lteqFilter(cf:String,col:String,value:Array[Byte]): Filter ={
    val f = new SingleColumnValueFilter(Bytes.toBytes(cf),Bytes.toBytes(col),CompareFilter.CompareOp.LESS_OR_EQUAL,value)
    f.setLatestVersionOnly(true)
    f.setFilterIfMissing(true)
    f
  }

  /**
    * 获得不等于过滤器。相当于SQL的 [字段] != [值]
    *
    * @param cf    列族名
    * @param col   列名
    * @param val   值
    * @return      过滤器
    */

  def neqFilter(cf: String, col: String, value: Array[Byte]): Filter ={
    val f = new SingleColumnValueFilter(Bytes.toBytes(cf),Bytes.toBytes(col),CompareFilter.CompareOp.NOT_EQUAL,value)
    f.setLatestVersionOnly(true)
    f.setFilterIfMissing(true)
    f
  }

  /**
    * 和过滤器 相当于SQL的 的 and
    *
    * @param filters   多个过滤器
    * @return          过滤器
    */

  def andFilter(filters: Filter*): Filter ={
    val filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);

    if (filters.length == 1){
      filters(0)
    }else{
      for (filter <- filters){
        filterList.addFilter(filter)
      }
      filterList
    }
  }

  /**
    * 和过滤器 相当于SQL的 的 and
    *
    * @param filters   多个过滤器
    * @return          过滤器
    */



  /**
    * 或过滤器 相当于SQL的 or
    *
    * @param filters   多个过滤器
    * @return          过滤器
    */

  def orFilter(filters:Filter*): Filter ={
    val filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE)
    for (filter <- filters){
      filterList.addFilter(filter)
    }
    filterList
  }

  /**
    * 或过滤器 相当于SQL的 or
    *
    * @param filters   多个过滤器
    * @return          过滤器
    */



  /**
    * 非空过滤器 相当于SQL的 is not null
    *
    * @param cf    列族
    * @param col   列
    * @return      过滤器
    */

  def notNullFilter(cf:String,col:String): Filter ={
    val f = new SingleColumnValueFilter(Bytes.toBytes(cf),Bytes.toBytes(col),CompareFilter.CompareOp.NOT_EQUAL,new NullComparator())
    f.setFilterIfMissing(true)
    f.setLatestVersionOnly(true)
    f
  }

  /**
    * 空过滤器 相当于SQL的 is null
    *
    * @param cf    列族
    * @param col   列
    * @return      过滤器
    */

  def nullFilter(cf:String,col:String): Filter ={
    val f = new SingleColumnValueFilter(Bytes.toBytes(cf),Bytes.toBytes(col),CompareFilter.CompareOp.EQUAL,new NullComparator())
    f.setFilterIfMissing(true)
    f.setLatestVersionOnly(true)
    f
  }

  /**
    * 子字符串过滤器 相当于SQL的 like '%[val]%'
    *
    * @param cf    列族
    * @param col   列
    * @param sub   子字符串
    * @return      过滤器
    */
  def subStringFilter(cf:String,col:String,sub:String): Filter ={
    val f = new SingleColumnValueFilter(Bytes.toBytes(cf),Bytes.toBytes(col),CompareFilter.CompareOp.EQUAL,new SubstringComparator(sub))
    f.setFilterIfMissing(true)
    f.setLatestVersionOnly(true)
    f
  }

  /**
    * 正则过滤器 相当于SQL的 rlike '[regex]'
    *
    * @param cf    列族
    * @param col   列
    * @param regex 正则表达式
    * @return      过滤器
    */

  def regexFilter(cf:String,col:String,regex:String): Filter ={
    val f = new SingleColumnValueFilter(Bytes.toBytes(cf),Bytes.toBytes(col),CompareFilter.CompareOp.EQUAL,new RegexStringComparator(regex))
    f.setFilterIfMissing(true)
    f.setLatestVersionOnly(true)
    f
  }

}
