package org.training.spark.learning

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext


object SparkJsonLoder {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName(getClass.getName)
    val sc = new SparkContext(conf)
    val sqlContext=new HiveContext(sc)
    import sqlContext.implicits._


    val jsonDF= sqlContext.read.format("json")
      //.option("inferSchema","true") by default interSChema is true for json files
      .load("src/main/resources/sales.json")

    //val amtByCust= jsonDF.groupBy("customerId").sum("amountPaid")

    val amtByCust= jsonDF.groupBy("customerId").agg(sum($"amountPaid").as("totalAmtPaid"), count ("itemId")
      .as("totalNoOfItems"),collect_set("itemId").as("itemsList"))  //collect_list method is not available in the sparkQL so we have to use hiveQL i.e HiveContext


    val discountDF=amtByCust.withColumn("discountPrice", when($"totalAmtPaid">2000, $"totalAmtPaid"*0.90).otherwise($"totalAmtPaid"))

    discountDF.show()

    Thread.sleep(100000)



    //compute total amount provide 10% discount if total amount>200

    //amtByCust.printSchema()
    //amtByCust.show()



  }

}
