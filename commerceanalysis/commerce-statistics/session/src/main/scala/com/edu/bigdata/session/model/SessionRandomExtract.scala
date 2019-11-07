package com.edu.bigdata.session.model

/**
  * Session 随机抽取表
  *
  * @param taskid           当前计算批次的 ID
  * @param sessionid        抽取的 Session 的 ID
  * @param startTime        Session 的开始时间
  * @param searchKeywords   Session 的查询字段
  * @param clickCategoryIds Session 点击的类别 id 集合
  */
case class SessionRandomExtract(taskid: String,
                                sessionid: String,
                                startTime: String,
                                searchKeywords: String,
                                clickCategoryIds: String)
