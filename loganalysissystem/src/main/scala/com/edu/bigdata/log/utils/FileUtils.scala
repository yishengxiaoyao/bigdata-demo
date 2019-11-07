package com.edu.bigdata.log.utils

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.internal.Logging

import scala.collection.mutable

object FileUtils extends Serializable with Logging{
  val storageFileFormat = "parquet"
  def moveTempFiles(fileSystem: FileSystem, outputPath: String, loadTime: String, template: String, partitions: mutable.HashSet[String]): Unit = {

  }

  def moveTempFiles(fileSystem: FileSystem, outputPath: String, loadTime: String, template: String, partitions: mutable.HashSet[String], isSmallFileMerge: Boolean): Unit = {
  }


  def makeCoalesce(fileSystem: FileSystem, filePath: String, coalesceSize: Int): Int = {
    0
  }

}
