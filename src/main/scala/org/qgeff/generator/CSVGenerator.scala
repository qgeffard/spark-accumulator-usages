package org.qgeff.generator

import org.apache.spark.sql.SparkSession


object CSVGenerator {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Generate dummy csv data file")
      .getOrCreate()

    import spark.implicits._

    val firstNames = Array(
      "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael",
      "Linda", "William", "Elizabeth", "David", "Barbara", "Richard", "Susan",
      "Joseph", "Jessica", "Thomas", "Sarah", "Charles", "Karen", "Emma",
      "Olivia", "Noah", "Liam", "Sophia", "Ava", "Isabella", "Mia"
    )

    val lastNames = Array(
      "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
      "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
      "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
      "Lee", "Thompson", "White", "Harris", "Clark", "Lewis", "Robinson"
    )

    val numRows = 10000000 // Change this to generate more or fewer records

    val data = spark.range(0, numRows).map { _ =>
      (
        firstNames(scala.util.Random.nextInt(firstNames.length)),
        lastNames(scala.util.Random.nextInt(lastNames.length)),
        scala.util.Random.nextInt(83) + 18 // Generate ages between 18 and 100
      )
    }.toDF("firstname", "lastname", "age")

    data.coalesce(1)
      .write
      .option("header", "true")
      .mode("overwrite")
      .csv("dummy_data")

    println("Sample of generated data:")
    data.show(5)

    spark.stop()
  }
}
