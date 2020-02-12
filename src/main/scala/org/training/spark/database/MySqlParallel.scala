package org.training.spark.database

import org.apache.spark.{SparkConf, SparkContext}

object MySqlParallel {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf()
    sparkConf.set("spark.driver.memory", "2g")
    val sc: SparkContext = new SparkContext(args(0), "spark_jdbc", sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val mysqlOption = Map("url" -> "jdbc:mysql://localhost:3306/ecommerce",
      "dbtable" -> "sales",
      "user" -> "root",
      "password" -> "cloudera",
      "fetchSize" -> "10",
      "partitionColumn" -> "customerId", "lowerBound" -> "1", "upperBound" -> "4", "numPartitions" -> "4")


    val jdbcDF = sqlContext.read
                           .format("org.apache.spark.sql.jdbc")
                           .options(mysqlOption)
                           .load()

    //jdbcDF.printSchema()

    jdbcDF.registerTempTable("sales")

    sqlContext.sql("SELECT transactionId, customerId, itemId, amountPaid from sales")
      //.write.mode("overwrite").json(args(1))
        .show

    Thread.sleep(10000)


  }
}
