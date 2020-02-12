package org.training.spark.learning

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.hive.HiveContext
object SparkNestedXMLLoader {

  def main(args: Array[String]): Unit = {

    val conf= new SparkConf().setAppName(getClass.getName).setMaster("local")
    val sc =new SparkContext(conf)
    val hiveContext=new HiveContext(sc)
    //val sqlContext= new SQLContext(sc)

    import hiveContext.implicits._

    val nestXMLDF= hiveContext.read.format("xml").option("inferSchema","true").option("rowTag","book").load("src/main/resources/books-nested-array.xml")

    val flatDF= nestXMLDF.withColumn("publish_date", explode($"publish_date") )
    val latestDF= flatDF.withColumn("rank",rank().over(Window.partitionBy("_id").orderBy($"publish_date".cast("date").desc)))
    val finalDF= latestDF.where("rank=1").drop("rank")


    finalDF.printSchema()
    finalDF.show()

  }

}


