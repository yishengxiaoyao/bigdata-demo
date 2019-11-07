package com.edu.bigdata.itemcf.model

// 商品相似度列表（商品相似度矩阵/商品推荐列表）
case class ProductRecs(productId: Int, recs: Seq[Recommendation])