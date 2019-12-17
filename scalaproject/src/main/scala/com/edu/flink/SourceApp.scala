package com.edu.flink

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object SourceApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    /*env.fromCollection(List(
      "234234","xiaoyao"
    )).print()*/
    env.addSource(new AccessSource())
    env.execute(this.getClass.getSimpleName)
  }
}
