package com.book.code.ml.transformation

import org.apache.spark.ml.feature.SQLTransformer
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object SQLTransformerExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SQLTransformerExample")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val df = sqlContext.createDataFrame(
      Seq((0, 1.0, 3.0), (2, 2.0, 5.0))).toDF("id", "v1", "v2")

    // 进行一个 sql 的转化，这里的 select 中，可以是 Field，Constant，UDF 等等
    val sqlTrans = new SQLTransformer().setStatement(
      "SELECT *, (v1 + v2) AS v3, (v1 * v2) AS v4 FROM __THIS__")

    sqlTrans.transform(df).show()

    sc.stop()
  }
}