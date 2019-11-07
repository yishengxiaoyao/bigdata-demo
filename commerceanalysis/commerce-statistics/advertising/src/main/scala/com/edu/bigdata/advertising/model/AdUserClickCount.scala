package com.edu.bigdata.advertising.model

/**
  * 用户广告点击量
  *
  * @author
  *
  */
case class AdUserClickCount(date: String,
                            userid: Long,
                            adid: Long,
                            clickCount: Long)