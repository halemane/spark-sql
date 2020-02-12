package org.training.spark.udfs

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.udf
import org.apache.spark.{SparkConf, SparkContext}

object UDFExamples {

  //def myToInt (input: Double) = input.toInt

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster(args(0)).setAppName("UDFExample")
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val salesDf = sqlContext.read.format("org.apache.spark.sql.json").load(args(1))
    salesDf.show()
    salesDf.registerTempTable("sales")

    val myToInt = udf((input:Double) => input.toInt)

    //sqlContext.udf.register("ConvertToInt", myToInt)

    //val results = sqlContext.sql("select customerId,ConvertToInt(amountPaid) from sales")
    import sqlContext.implicits._
    salesDf.select($"customerId", myToInt($"amountPaid")).show()
    //results.show()

  }

}
