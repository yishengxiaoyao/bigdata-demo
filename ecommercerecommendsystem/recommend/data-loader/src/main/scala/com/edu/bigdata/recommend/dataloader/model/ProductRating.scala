package com.edu.bigdata.recommend.dataloader.model

case class ProductRating(userId: Int, productId: Int, score: Double, timestamp: Int)