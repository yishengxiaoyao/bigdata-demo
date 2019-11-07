package com.book.code.ml.selection

import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NumericAttribute}
import org.apache.spark.ml.feature.VectorSlicer
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object VectorSlicerExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("VectorSlicerExample")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val data = Array(Row(Vectors.dense(-2.0, 2.3, 0.0)))

    val defaultAttr = NumericAttribute.defaultAttr
    val attrs = Array("f1", "f2", "f3").map(defaultAttr.withName)
    val attrGroup = new AttributeGroup("userFeatures", attrs.asInstanceOf[Array[Attribute]])

    val dataRDD = sc.parallelize(data)
    val dataset = sqlContext.createDataFrame(dataRDD, StructType(Array(attrGroup.toStructField())))

    val slicer = new VectorSlicer().setInputCol("userFeatures").setOutputCol("features")

    slicer.setIndices(Array(1)).setNames(Array("f3"))
    // or slicer.setIndices(Array(1, 2)), or slicer.setNames(Array("f2", "f3"))

    val output = slicer.transform(dataset)

    println(output.select("userFeatures", "features").first())

    sc.stop()
  }
}