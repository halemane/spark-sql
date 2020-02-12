package org.training.spark.learning

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._  ///to apply the functions on top the columns

object SparkXMLLoader {

  def main(args: Array[String]): Unit = {

    val conf= new SparkConf().setMaster("local").setAppName(getClass.getName)
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val optionsMap= Map("header" -> "true", "inferSchema" -> "true" )

   val xmlDF= sqlContext.read.format("xml")
     .option("rowTag","person")
     .load("src/main/resources/ages.xml")

    xmlDF.registerTempTable("v_xml")

    import sqlContext.implicits._   //inline import:- this can be used only upon the creation of SQLContext object.

    val xmlDFNew= xmlDF.select( xmlDF("name").as("Name"),col("age._born").alias("DOB"),column("age._birthplace").as("Place"),$"age._VALUE".alias("Age"))


    xmlDFNew.registerTempTable("v_xmlDFNew")
    //xmlDF.selectExpr("upper(name) as Name","trim(age._born) as DOB","age._birthplace as BirthPlace","age._VALUE as Age").show()


    //val xmlDFNew = sqlContext.sql("SELECT name, age._born as dob,age._birthplace as birthPlace,age._VALUE as age from v_xml")

    val newColDF=xmlDFNew.withColumn("locationFlag", when($"Place" isNotNull, "Y").otherwise("N"))


    // IF AGE IS <20 TEENAGE, BETWEEN 20 AND 50 AS ADULT and GREATERTHAN 50 OLDAGE
    val ageCatColDF= newColDF.withColumn("ageCat",when($"Age"<=20, "TeenAge").when( $"Age">20 and $"Age"<=50, "Adult"

    ).otherwise("OldAge"))

    val renamedDF= ageCatColDF.withColumnRenamed("DOB","DateOfBirth").drop("ageCat")


    renamedDF.show()
    renamedDF.printSchema()


  }

}
