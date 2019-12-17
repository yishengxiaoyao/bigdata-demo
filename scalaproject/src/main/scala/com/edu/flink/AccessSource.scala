package com.edu.flink

import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.util.Random

class AccessSource extends SourceFunction[Access]{
  var running = false
  override  def run(ctx: SourceFunction.SourceContext[Access]): Unit = {
    while (running){
      val timestamp = System.currentTimeMillis()
      1.to(10).map(x=>{
        ctx.collect(Access(timestamp))
      })
    }

  }

  override def cancel(): Unit = {
    running = false
  }
}
