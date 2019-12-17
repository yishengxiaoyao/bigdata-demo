package com.edu.flink

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, RichSourceFunction, SourceFunction}

object AcessSource3 extends RichParallelSourceFunction[Access]{
  override def run(ctx: SourceFunction.SourceContext[Access]): Unit = {

  }

  override def cancel(): Unit = {

  }

  override def open(parameters: Configuration): Unit = {

  }


}
