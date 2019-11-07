package com.edu.bigdata.session.model

/**
  * 品类 Top10 表
  *
  * @param taskid
  * @param categoryid
  * @param clickCount
  * @param orderCount
  * @param payCount
  */
case class Top10Category(taskid: String,
                         categoryid: Long,
                         clickCount: Long,
                         orderCount: Long,
                         payCount: Long)