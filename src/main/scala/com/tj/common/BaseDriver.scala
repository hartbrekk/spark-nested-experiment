package com.tj.common

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

trait BaseDriver extends App {

  private[this] val c = ConfigFactory.load()

  protected val appName = c.getString("app.name")

  protected val dataDir = c.getString("app.dataDir")

  protected val sparkMasterUrl = c.getString("spark.masterUrl")

  protected val tempDir = c.getString("spark.tempDir")

  //System.setProperty("hadoop.home.dir", "c:/spark/winutils/");

  protected val sparkConf = new SparkConf()
    .setAppName(appName)
    .setMaster(sparkMasterUrl)
    .set("spark.executor.memory", "3g")
    .set("spark.sql.shuffle.partitions", "4")

  protected val sparkContext = SparkContext.getOrCreate(sparkConf)
  sparkContext.setLogLevel(c.getString("spark.logLevel"));
  println("############################SPARK CONFIG##############################")
  sparkContext.getConf.getAll.foreach(println(_))

  protected val sqlContext = SQLContext.getOrCreate(sparkContext)
  println("############################SPARK SQL CONFIG##############################")
  sqlContext.getAllConfs.foreach(println(_))

}
