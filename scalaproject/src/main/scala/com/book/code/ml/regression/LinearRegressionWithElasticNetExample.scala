package com.book.code.ml.regression

import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object LinearRegressionWithElasticNetExample {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: <regression_data>")
      System.exit(1)
    }

    val inputData = args(0)

    val conf = new SparkConf().setAppName("LinearRegressionWithElasticNetExample")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // 加载训练数据
    val training = sqlContext.read.format("libsvm").load(inputData)

    val lr = new LinearRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    // 拟合模型
    val lrModel = lr.fit(training)

    // 打印相关系数和 linear regression 相关的解释
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    // 对训练数据上的模型进行 Summarize，打印一些 metrics
    val trainingSummary = lrModel.summary

    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: ${trainingSummary.objectiveHistory.toList}")

    trainingSummary.residuals.show()

    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")

    sc.stop()
  }
}