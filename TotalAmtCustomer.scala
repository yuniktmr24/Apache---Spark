package com.sundogsoftware.spark.solutions

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object TotalAmtCustomer {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local", "TotalAmtCustomer")
    val input = sc.textFile("./data/customer-orders.csv") //adjust path accordingly

    val inputTokenized = input.map(x => {
      val fields = x.split(",")
      (fields(0).toInt, fields(2).toFloat)
    })

    val amtSpent = inputTokenized.reduceByKey((x1, x2) => x1 + x2)
    val amtSpentResults = amtSpent.collect()

    for (amt <- amtSpentResults) {
      println(amt)
    }

    val amtSpentRev = amtSpent.map((x1) => (x1._2, x1._1))
    val results = amtSpentRev.sortByKey()
    val resultsList = results.collect()

    results.foreach(println)
  }

}
