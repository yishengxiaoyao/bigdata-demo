package com.book.code

/*
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.{SparkConf, SparkContext}

object HBaseTest {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: <table_name>")
      System.exit(1)
    }

    // please ensure HBASE_CONF_DIR is on classpath of spark driver
    // e.g: set it through spark.driver.extraClassPath property
    // in spark-defaults.conf or through --driver-class-path
    // command line option of spark-submit
    val conf = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE, args(0))

    val sparkConf = new SparkConf().setAppName("HBaseTest")
    val sc = new SparkContext(sparkConf)

    // Initialize hBase table if necessary
    val admin = new HBaseAdmin(conf)
    if (!admin.isTableAvailable(args(0))) {
      val tableDesc = new HTableDescriptor(TableName.valueOf(args(0)))
      admin.createTable(tableDesc)
    }

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    hBaseRDD.take(10).foreach(x => println(s"${x._1.toString}\t${x._2.toString}"))
    hBaseRDD.count()

    sc.stop()
    admin.close()
  }
}
*/