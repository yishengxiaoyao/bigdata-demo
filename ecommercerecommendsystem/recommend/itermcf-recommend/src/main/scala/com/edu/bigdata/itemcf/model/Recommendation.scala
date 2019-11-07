package com.edu.bigdata.itemcf.model

// 标准推荐对象，productId, score
case class Recommendation(productId: Int, score: Double)