package com.edu.bigdata.online.model

// 用户推荐列表
case class UserRecs(userId: Int, recs: Seq[Recommendation])