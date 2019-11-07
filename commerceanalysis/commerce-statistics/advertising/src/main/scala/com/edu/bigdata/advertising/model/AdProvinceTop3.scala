package com.edu.bigdata.advertising.model

/**
  * 各省 top3 热门广告
  *
  * @author
  *
  */
case class AdProvinceTop3(date: String,
                          province: String,
                          adid: Long,
                          clickCount: Long)