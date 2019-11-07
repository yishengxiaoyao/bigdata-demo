package com.edu.bigdata.advertising.model

/**
  * 广告实时统计
  *
  * @author
  *
  */
case class AdStat(date: String,
                  province: String,
                  city: String,
                  adid: Long,
                  clickCount: Long)