package org.qgeff.usages

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{expr, lit}

object CountVSAccumulator extends App {

  private def withoutAccumulator(): Unit = {
    val spark = SparkSession
      .builder
      .appName("Spark to SQLite")
      .master("local")
      .getOrCreate

    // Sample DataFrame (replace this with your actual DataFrame)
    val df = spark.read.option("header", "true").csv("dummy_data")

    val startTime = System.nanoTime()

    // SQLite JDBC URL format
    val jdbcUrl = "jdbc:sqlite:sqlite/database.db"

    val dfFiltered = df.filter(expr("age > 50")).withColumn("senior", lit(true))

    println(s" Count : ${dfFiltered.count()}")

    // Writing DataFrame to SQLite
    dfFiltered.write
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", "mytable")
      .option("driver", "org.sqlite.JDBC")
      .mode("overwrite")
      .save()

    val endTime = System.nanoTime()
    val durationMs = (endTime - startTime) / 1000000000.0
    println(s"Operation took $durationMs seconds")
  }

  private def withAccumulator(): Unit = {
    val spark = SparkSession
      .builder
      .appName("Spark to SQLite")
      .master("local")
      .getOrCreate

    // Create an accumulator to count rows after filtering
    val filteredRowCount = spark.sparkContext.longAccumulator("Filtered Row Count")


    // Sample DataFrame (replace this with your actual DataFrame)
    val df = spark.read.option("header", "true").csv("dummy_data")

    val startTime = System.nanoTime()

    // SQLite JDBC URL format
    val jdbcUrl = "jdbc:sqlite:sqlite/database.db"

    val dfFiltered = df.filter(row => {
      val bool = row.getAs[String]("age").toInt > 50
      if(bool)
        filteredRowCount.add(1)
      bool
    }).withColumn("senior", lit(true))


    // Writing DataFrame to SQLite
    dfFiltered.write
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", "mySecondTable")
      .option("driver", "org.sqlite.JDBC")
      .mode("overwrite")
      .save()

    println(s" Count : ${filteredRowCount.value}")

    val endTime = System.nanoTime()
    val durationMs = (endTime - startTime) / 1000000000.0
    println(s"Operation took $durationMs seconds")
  }

  withoutAccumulator()
  println(" -- ")
  withAccumulator()
}
