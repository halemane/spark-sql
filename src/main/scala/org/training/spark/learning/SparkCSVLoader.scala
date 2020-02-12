package org.training.spark.learning

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object SparkCSVLoader {

  def main(args: Array[String]): Unit = {

    val conf= new SparkConf().setMaster("local").setAppName(getClass.getName)
    val sc = new SparkContext(conf)
    val sqc = new SQLContext(sc)

    val optionsMap= Map("header" -> "true", "inferSchema" -> "true", "delimiter"->",")

    val csvDF= sqc.read.format("csv")
      //.option("header","true")
      //.option("inferSchema","true")
      .options(optionsMap)
      .load("/home/cloudera/Desktop/purchase_data.csv").registerTempTable("v_csv")



/*    val csvDFNew= sqc.sql("SELECT tranId,custId from v_csv WHERE amountPaid>1000 ")

    val custWiseSalesAmt= sqc.sql("SELECT custId,SUM(amountPaid)as custAmt FROM v_csv GROUP BY custId")

    custWiseSalesAmt.show()*/





  }


}
