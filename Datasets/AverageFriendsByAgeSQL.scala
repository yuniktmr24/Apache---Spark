package com.sundogsoftware.spark.solutions.Datasets

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object AverageFriendsByAgeSQL {
  case class Person(id: Int, name: String, age: Int, friends: Int)

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    // Convert our csv file to a DataSet, using our Person case
    // class to infer the schema.
    import spark.implicits._
    val people = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/fakefriends.csv")
      .as[Person]

    //using SparkSQL
    println("Using SparkSQL")
    people.printSchema()

    people.createOrReplaceTempView("people")
    println("Let's select the age, friends column:")
    val cols = spark.sql("SELECT age, friends from people")
    val colsRes = cols.collect()
    colsRes.foreach(println)
    println("Average friends Group by age:")
    val averageFriends = spark.sql("SELECT avg(friends) as avg_friends, age from people GROUP BY age ORDER by age ASC")
    val averageFriendsRes = averageFriends.collect()
    averageFriendsRes.foreach(println)

    spark.stop()

  }
}
