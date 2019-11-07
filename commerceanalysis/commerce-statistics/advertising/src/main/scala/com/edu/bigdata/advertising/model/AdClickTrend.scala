package com.edu.bigdata.advertising.model

 /**
  * 广告点击趋势
  *
  * @author
  *
  */
case class AdClickTrend(date: String,
                        hour: String,
                        minute: String,
                        adid: Long,
                        clickCount: Long)