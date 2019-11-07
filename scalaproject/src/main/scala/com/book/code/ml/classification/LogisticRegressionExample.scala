package com.book.code.ml.classification

import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object LogisticRegressionExample {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: <regression_data>")
      System.exit(1)
    }

    val inputData = args(0)

    val conf = new SparkConf().setAppName("LogisticRegressionExample")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val training = sqlContext.read.format("libsvm").load(inputData)

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    // 模型训练
    val lrModel = lr.fit(training)

    // 输出模型的相关系数和解释器
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    // 从 LogisticRegressionModel 中抽取出一些 summary 信息
    val trainingSummary = lrModel.summary

    val objectiveHistory = trainingSummary.objectiveHistory
    objectiveHistory.foreach(loss => println(loss))

    // 用于获取一些测试的指标
    val binarySummary = trainingSummary.asInstanceOf[BinaryLogisticRegressionSummary]

    // 打印 AUC 数值
    val roc = binarySummary.roc
    roc.show()
    println(binarySummary.areaUnderROC)

    // 设置模型阈值，最大化 F-Measure
    val fMeasure = binarySummary.fMeasureByThreshold
    val maxFMeasure = fMeasure.select(max("F-Measure")).head().getDouble(0)
    val bestThreshold = fMeasure.where($"F-Measure" === maxFMeasure)
      .select("threshold").head().getDouble(0)

    lrModel.setThreshold(bestThreshold)

    // 显示出最佳的阈值
    println(s"bestThreshold: $bestThreshold")

    // 进行预测
    lrModel.transform(training).select("label", "probability", "prediction")
      .collect()
      .foreach { case Row(label: Double, prob: Vector, prediction: Double) =>
        println(s"($label) -> prob=$prob, prediction=$prediction")
      }

    sc.stop()
  }
}