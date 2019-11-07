package com.edu.bigdata.offline.model

// 用户推荐列表
case class UserRecs(userId: Int, recs: Seq[Recommendation])