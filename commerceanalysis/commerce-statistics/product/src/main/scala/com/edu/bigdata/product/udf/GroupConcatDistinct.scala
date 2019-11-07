package com.edu.bigdata.product.udf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}

/**
  * 自定义弱类型的聚合函数（UDAF）
  */
class GroupConcatDistinct extends UserDefinedAggregateFunction{
  // 设置 UDAF 函数的输入类型为 String
  override def inputSchema: StructType = StructType(StructField("cityInfoInput", StringType) :: Nil)

  // 设置 UDAF 函数的缓冲区类型为 String
  override def bufferSchema: StructType = StructType(StructField("cityInfoBuffer", StringType) :: Nil)

  // 设置 UDAF 函数的输出类型为 String
  override def dataType: DataType = StringType

  // 设置一致性检验，如果为 true，那么输入不变的情况下计算的结果也是不变的
  override def deterministic: Boolean = true

  // 初始化自定义的 UDAF 函数
  // 设置聚合中间 buffer 的初始值
  // 需要保证这个语义：两个初始 buffer 调用下面实现的 merge 方法后也应该为初始 buffer，
  // 即如果你初始值是1，然后你 merge 是执行一个相加的动作，两个初始 buffer 合并之后等于 2，不会等于初始 buffer 了，
  // 这样的初始值就是有问题的，所以初始值也叫"zero value"
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = ""
  }

  // 设置 UDAF 函数的缓冲区更新：实现一个字符串带去重的拼接
  // 用输入数据 input 更新 buffer 值，类似于 combineByKey
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    // 缓冲中的已经拼接过的城市信息串
    var cityInfoBuffer = buffer.getString(0)
    // 刚刚传递进来的某个城市信息
    val cityInfoInput = input.getString(0)

    // 在这里要实现去重的逻辑
    // 判断：之前没有拼接过某个城市信息，那么这里才可以接下去拼接新的城市信息
    if (!cityInfoBuffer.contains(cityInfoInput)) {
      if ("".equals(cityInfoBuffer)) {
        cityInfoBuffer += cityInfoInput
      } else {
        // 比如 1:北京
        // 1:北京,2:上海
        cityInfoBuffer += "," + cityInfoInput
      }
    }

    buffer.update(0, cityInfoBuffer)
  }

  // 把两个自定义的 UDAF 函数的值合并在一起
  // 合并两个 buffer, 将 buffer2 合并到 buffer1. 在合并两个分区聚合结果的时候会被用到, 类似于 reduceByKey
  // 这里要注意该方法没有返回值，在实现的时候是把 buffer2 合并到 buffer1 中去，你需要实现这个合并细节
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    // cityInfoBuffer1: cityId1:cityName1, cityId2:cityName2, cityId3:cityName3, ...
    var cityInfoBuffer1 = buffer1.getString(0)
    // cityInfoBuffer2: cityId1:cityName1, cityId2:cityName2, cityId3:cityName3, ...
    val cityInfoBuffer2 = buffer2.getString(0)

    // 将 cityInfoBuffer2 中的数据带去重的加入到 cityInfoBuffer1 中
    for (cityInfo <- cityInfoBuffer2.split(",")) {
      if (!cityInfoBuffer1.contains(cityInfo)) {
        if ("".equals(cityInfoBuffer1)) {
          cityInfoBuffer1 += cityInfo
        } else {
          cityInfoBuffer1 += "," + cityInfo
        }
      }
    }

    buffer1.update(0, cityInfoBuffer1)
  }

  // 计算并返回最终的聚合结果
  override def evaluate(buffer: Row): Any = {
    buffer.getString(0)
  }

}
