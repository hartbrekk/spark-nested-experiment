package com.tj.app

import com.tj.common.BaseDriver

object NestedRdd extends BaseDriver {

  import sqlContext.implicits._

  val empRdd = sparkContext.parallelize(Seq((1, "ABC", 50), (1, "DEF", 40)))

  val empDF = empRdd.toDF("Dept", "Name", "Age")

  //val empDF = sqlContext.read.json("file://" + dataDir + "/formats/json/employee.json")

  //empDF.printSchema()

  import org.apache.spark.sql.functions._

  //val newDF = empDF.groupBy($"Dept").agg(collect_list($"Name")).collect()
  //val empDS = empDF.as[Emp]

  //empDS.groupBy(_.dept).

  val mappedRdd = empDF.map(row => row(0) -> (row(1), row(2)))

  //val res = mappedRdd.groupByKey().map{ case(x, y) => (x, y.toList) }.collect

  //res.foreach(println(_))


 // val res = mappedRdd.groupByKey().map { case (x, y) => (x, y.toList) }.saveAsTextFile()

  //mappedRdd.

  //res.foreach(println(_))
  //res.write.json(dataDir + "/formats/json/employee-nested.json")
  //res.write.parquet(dataDir + "/formats/json/employee-nested.parquet")
//https://stackoverflow.com/questions/45131481/aggregation-function-collect-list-or-collect-set-over-window
  // https://stackoverflow.com/questions/37737843/aggregating-multiple-columns-with-custom-function-in-spark

}

case class Emp(dept: Int, name: String, age: Int)
