package com.edu.bigdata.session.model

/**
  * Top10 Session
  *
  * @param taskid
  * @param categoryid
  * @param sessionid
  * @param clickCount
  */
case class Top10Session(taskid: String,
                        categoryid: Long,
                        sessionid: String,
                        clickCount: Long)
