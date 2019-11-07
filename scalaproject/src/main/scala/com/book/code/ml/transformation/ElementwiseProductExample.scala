package com.book.code.ml.transformation

/*
import org.apache.spark.ml.feature.ElementwiseProduct
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

object ElementwiseProductExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ElementwiseProductExample")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // 建立向量数据，也支持 "稀疏" 向量
    val dataFrame = sqlContext.createDataFrame(Seq(
      ("a", Vectors.dense(1.0, 2.0, 3.0)),
      ("b", Vectors.dense(4.0, 5.0, 6.0)))).toDF("id", "vector")

    val transformingVector = Vectors.dense(0.0, 1.0, 2.0)
    val transformer = new ElementwiseProduct()
      .setScalingVec(transformingVector)
      .setInputCol("vector")
      .setOutputCol("transformedVector")

    // 将 dataFrame 进行转换，其实是对 dataFrame 和 transformingVector 进行计算，计算方式也是比较直接的，
    // 即对 vector 中的每个元素对应进行乘积计算
    transformer.transform(dataFrame).show()

    sc.stop()
  }
}*/