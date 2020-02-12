name := "spark-sql"

version := "0.1"

scalaVersion := "2.10.5"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.3"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.3"

libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.6.3"

libraryDependencies += "com.databricks" %% "spark-csv" % "1.5.0"

libraryDependencies += "com.databricks" %% "spark-xml" % "0.4.1"

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.47"

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "1.6.13"
