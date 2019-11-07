package com.edu.bigdata.itemcf.model

// 注意：spark mllib 中有 Rating 类，为了便于区别，我们重新命名为 ProductRating
case class ProductRating(userId: Int, productId: Int, score: Double, timestamp: Int)