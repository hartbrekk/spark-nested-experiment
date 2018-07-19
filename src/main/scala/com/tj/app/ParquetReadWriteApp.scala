package com.tj.app

import com.tj.common.BaseDriver

object ParquetReadWriteApp extends BaseDriver {

  import sqlContext.implicits._

  println("Dataset Schema:")
  val retailDataDS = sqlContext.read.option("inferSchema", "true").json(dataDir + "/retaildata.json").as[RetailData]
  retailDataDS.printSchema

  println("Grouping by:")
  val nestedDS = retailDataDS.groupBy(_.country).mapGroups { case (x, y) => (x, y.toSeq) }
  nestedDS.printSchema()
  nestedDS.cache()
  nestedDS.show(5)


  println("Writing out Parquet file ...")
  nestedDS.toDF.write.parquet(dataDir + "/nestedDS16.parquet")

  println("Output DataFrame verify ...")
  val outputDF = sqlContext.read.parquet(dataDir + "/nestedDS16.parquet")
  outputDF.printSchema

  println("Output DataSet verify ...")
  val outputDS = sqlContext.read.parquet(dataDir + "/nestedDS16.parquet").as[RetailDataByCountry]
  outputDS.printSchema
  outputDS.show

  // POC
  // val actualDF = retailDataDF.groupBy("Country").agg(collect_list(struct("Description","InvoiceNo")))
  // val nestedDS = sqlContext.read.parquet(dataDir + "/nested16.parquet").as[InvoiceByCountry]
  // nestedDS.write.parquet(dataDir + "/nested16.parquet")
  // nestedDS.write.json(dataDir + "/nested16.json")
}

case class RetailData(country: String, customerID: Double, description: String, invoiceDate: String, invoiceNo: String, quantity: Long, stockCode: String, unitPrice: Double) extends Serializable {}

/*
root
|-- Country: string (nullable = true)
|-- CustomerID: double (nullable = true)
|-- Description: string (nullable = true)
|-- InvoiceDate: string (nullable = true)
|-- InvoiceNo: string (nullable = true)
|-- Quantity: long (nullable = true)
|-- StockCode: string (nullable = true)
|-- UnitPrice: double (nullable = true)
*/

case class RetailDataByCountry(country: String, customList: Seq[RetailData]) extends Serializable {}

case class InvoiceByCountry(country: String, nested: Seq[Invoice]) extends Serializable {}

/*
|-- Country: string (nullable = true)
 |-- nested: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- Description: string (nullable = true)
 |    |    |-- InvoiceNo: string (nullable = true)

 */
case class Invoice(description: String, invoiceNo: String) extends Serializable {}
