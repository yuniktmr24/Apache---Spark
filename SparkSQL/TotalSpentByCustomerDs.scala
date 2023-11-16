package com.sundogsoftware.spark.solutions.SparkSQL



import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructType}
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/** Find the minimum temperature by weather station */
object TotalSpentByCustomerDs {

  case class Customer(userID: String, itemId: String, price: Float)

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession using every core of the local machine
    val spark = SparkSession
      .builder
      .appName("TotalAmtSpent")
      .master("local[*]")
      .getOrCreate()

    val amtSpentSchema = new StructType()
      .add("userID", StringType, nullable = true)
      .add("itemId", StringType, nullable = true)
      .add("price", FloatType, nullable = true)

    // Read the file as dataset
    import spark.implicits._
    val ds = spark.read
      .schema(amtSpentSchema)
      .csv("data/customer-orders.csv")
      .as[Customer]

    //approach 1
    /*
    val userAmt = ds.select("userID", "price")

    // Aggregate to find minimum temperature for every station
    val userAmtByUserID = userAmt.groupBy("userID").sum("price")

    // Convert temperature to fahrenheit and sort the dataset
    val sumByUser = userAmtByUserID
      .withColumn("amount_spent", round($"sum(price)", 2))
      .select("userID", "amount_spent").sort("amount_spent")

    // Collect, format, and print the results
    val results = sumByUser.collect()

    for (result <- results) {
      val userID = result(0)
      val amt = result(1).asInstanceOf[Double]

      println(s"$userID Amt Spent: $amt")
    }
    */


    //approach 2
    val userAmtByUserID = ds.groupBy("userID").agg(round(sum("price"), 2).alias("total_spent"))
    val totalByCustSorted = userAmtByUserID.sort("total_spent")

    totalByCustSorted.show(userAmtByUserID.count.toInt)
  }
}