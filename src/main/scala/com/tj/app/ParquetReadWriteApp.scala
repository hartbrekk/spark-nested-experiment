package com.tj.app

import java.time.LocalDateTime

import com.tj.common.BaseDriver

object ParquetReadWriteApp extends BaseDriver {

  import sqlContext.implicits._

  //val retailDataDF = sqlContext.read.option("inferSchema", "true").json(dataDir + "/retaildata.json")
  //retailDataDF.printSchema
  //retailDataDF.show


  //val userDF = sqlContext.read.parquet(dataDir + "/nested16.parquet") //.as[UserDataPq]
  //userDF.printSchema
  //userDF.show

  println("Checking DS now")
  val userDS = sqlContext.read.parquet(dataDir + "/nested16.parquet").as[InvoiceByCountry]
  userDS.printSchema
  userDS.show
  //val userDS = userDF.as[UserDataPq]

  //val userDS = userDF.as[UserDataPq]

  //userDS.collect().foreach(println(_))

  //userDS.write.parquet(dataDir + "/nested16.parquet")
  //userDS.write.json(dataDir + "/nested16.json")
  import org.apache.spark.sql.functions._
  //POC
  //val actualDF = retailDataDF.groupBy("Country").agg(collect_list(struct("Description","InvoiceNo")))
  //actualDF.printSchema()
  //actualDF.show()


}

case class Automobile(make: String,
                      fuelType: String,
                      aspire: String,
                      doors: String,
                      body: String,
                      drive: String,
                      cylinders: String,
                      hp: Int,
                      rpm: Int,
                      mpgcity: Int,
                      mpghwy: Int,
                      price: BigDecimal)

case class UserDataPq(registrationDttm: String,
                      id: Int,
                      firstName: String,
                      lastName: String,
                      email: String,
                      gender: String,
                      ipAddress: String,
                      cc: String,
                      country: String,
                      birthdate: String,
                      salary: Double,
                      title: String,
                      comments: String)

case class InvoiceByCountry(country: String, nested: Seq[Invoice])

case class Invoice(description: String, invoiceNo: String)


/*
|-- Country: string (nullable = true)
 |-- nested: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- Description: string (nullable = true)
 |    |    |-- InvoiceNo: string (nullable = true)

 */