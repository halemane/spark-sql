package org.training.spark.datasource

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._


object AgesXmlParserApp extends App {

  val sparkConf = new SparkConf()
  val sc = new SparkContext(args(0), "xml file", sparkConf)
  val sqlContext = new SQLContext(sc)

  val xmlDF = sqlContext.read.format("xml").
    option("rowTag", "person").
    load(args(1))

  xmlDF.printSchema()
  xmlDF.show()

  import sqlContext.implicits._
  val df1 = xmlDF.select(xmlDF("name").alias("name"),
    $"age._VALUE".as("actualage"),
    col("age._born").alias("birthdate"))
  df1.show

  val groupedDF = df1.groupBy(col("actualage")).count()
  groupedDF.show

}
