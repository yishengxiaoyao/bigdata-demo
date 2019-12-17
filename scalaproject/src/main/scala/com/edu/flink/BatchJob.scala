package com.edu.flink

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

object BatchJob {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val text = env.readTextFile("")
    text.flatMap(_.split(",")).map((_,1)).groupBy(0).sum(1).print()
    env.execute(this.getClass.getSimpleName)

  }
}
