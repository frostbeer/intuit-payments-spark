package com.intuit.payments.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Payments")
      .getOrCreate()

    import spark.implicits._

    val currencyILSConversionsRatesDF = Seq[(String, Double)](("EUR", 3.78),("USD", 3.54),("ILS", 1.0)).toDF("currency", "rate")
    val paymentsDF = spark.read.parquet("payments-parquet")

    paymentsDF.show(true)

    paymentsDF.groupBy("currency").agg(sum("amount").as("total")).show(true)

    paymentsDF.join(broadcast(currencyILSConversionsRatesDF), Seq("currency"))
      .withColumn("amount_in_ils", ceil(col("amount") * col("rate"), lit(2)))
      .drop("rate")
      .show(true)
  }
}
