package com.book.code.ml.cluster

/*
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object LDAExample {
  val FEATURES_COL = "features"

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: <input>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("LDAExample")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val input = args(0)

    // Loads data
    val rowRDD = sc.textFile(input).filter(_.nonEmpty)
      .map(_.split(" ").map(_.toDouble)).map(Vectors.dense).map(Row(_))

    val schema = StructType(Array(StructField(FEATURES_COL, new VectorUDT, false)))
    val dataset = sqlContext.createDataFrame(rowRDD, schema)

    dataset.printSchema()
    dataset.show(20)

    // Trains a LDA model
    val lda = new LDA()
      .setK(10)
      .setMaxIter(10)
      .setFeaturesCol(FEATURES_COL)

    val model = lda.fit(dataset)
    val transformed = model.transform(dataset)

    val ll = model.logLikelihood(dataset)
    val lp = model.logPerplexity(dataset)

    // describeTopics
    val topics = model.describeTopics(3)

    // Shows the result
    topics.show(false)
    transformed.show(false)

    sc.stop()
  }
}*/