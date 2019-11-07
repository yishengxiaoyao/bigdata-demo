package com.practice

import org.apache.spark.{SparkConf, SparkContext}

object AcculatorApp {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Test").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    //correct(sc)
    //shaojia(sc)
    //duojia(sc)
    huancun(sc)
    sc.stop()
  }

  def correct(sc:SparkContext): Unit ={
    val accum = sc.longAccumulator("longAccum") //统计奇数的个数
    val sum = sc.parallelize(Array(1,2,3,4,5,6,7,8,9),2).filter(n=>{
      if(n%2!=0) accum.add(1L)
      n%2==0
    }).reduce(_+_)

    println("sum: "+sum)
    println("accum: "+accum.value)

    sc.stop()
  }

  /**
    * 只有transformation，没有action操作，不触发accumulator进行操作
    * @param sc
    */
  def shaojia(sc:SparkContext): Unit ={
    val accum = sc.longAccumulator("longAccum")
    val numberRDD = sc.parallelize(Array(1,2,3,4,5,6,7,8,9),2).map(n=>{
      accum.add(1L)
      n+1
    })
    println("accum: "+accum.value)
  }

  /**
    * 多次调用action操作，因为调用action操作才会触发transformation操作，会多次计算，可以使用缓存
    * 方式来防止这个情况
    * @param sc
    */

  def duojia(sc: SparkContext): Unit ={
    val accum = sc.longAccumulator("longAccum")
    val numberRDD = sc.parallelize(Array(1,2,3,4,5,6,7,8,9),2).map(n=>{
      accum.add(1L)
      n+1
    })
    numberRDD.count
    println("accum1:"+accum.value)
    numberRDD.reduce(_+_)
    println("accum2: "+accum.value)
  }

  /**
    * 将map的结果缓存到内存中，在获取累加器时，不需要进行再次执行map操作。
    * @param sc
    */
  def huancun(sc: SparkContext): Unit ={
    val accum = sc.longAccumulator("longAccum")
    val numberRDD = sc.parallelize(Array(1,2,3,4,5,6,7,8,9),2).map(n=>{
      accum.add(1L)
      n+1
    })
    println(numberRDD.cache().count)
    println("accum1:"+accum.value)
    numberRDD.reduce(_+_)
    println("accum2: "+accum.value)
  }

}
