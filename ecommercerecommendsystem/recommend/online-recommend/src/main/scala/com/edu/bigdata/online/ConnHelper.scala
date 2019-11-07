package com.edu.bigdata.online

import com.mongodb.casbah.{MongoClient, MongoClientURI}
import redis.clients.jedis.Jedis
// 定义样例类

// 连接助手对象（用于建立 redis 和 mongo 的连接）并序列化
object ConnHelper extends Serializable {
  // 懒变量：使用的时候才初始化
  lazy val jedis = new Jedis("hadoop102")
  // 用于 MongoDB 中的一些复杂操作（读写之外的操作）
  lazy val mongoClient = MongoClient(MongoClientURI("mongodb://hadoop102:27017/ECrecommender"))
}
