package com.epam.hubd.spark.scala.sql.homework

import org.apache.spark.sql.{DataFrame, _}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

object MotelsHomeRecommendation {

  val ERRONEOUS_DIR: String = "erroneous"
  val AGGREGATED_DIR: String = "aggregated"

  def main(args: Array[String]): Unit = {
    require(args.length == 4, "Provide parameters in this order: bidsPath, motelsPath, exchangeRatesPath, outputBasePath")

    val bidsPath = args(0)
    val motelsPath = args(1)
    val exchangeRatesPath = args(2)
    val outputBasePath = args(3)

    val sc = new SparkContext(new SparkConf().setAppName("motels-home-recommendation").setMaster("local[4]"))
    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._

    processData(sqlContext, bidsPath, motelsPath, exchangeRatesPath, outputBasePath)

    sc.stop()
  }

  def processData(sqlContext: HiveContext, bidsPath: String, motelsPath: String, exchangeRatesPath: String, outputBasePath: String) = {

    /**
      * Task 1:
      * Read the bid data from the provided file.
      */
    val rawBids: DataFrame = getRawBids(sqlContext, bidsPath)

    /**
      * Task 1:
      * Collect the errors and save the result.
      */
    val erroneousRecords: DataFrame = getErroneousRecords(rawBids)
    erroneousRecords.write
      .format(Constants.CSV_FORMAT)
      .save(s"$outputBasePath/$ERRONEOUS_DIR")

    /**
      * Task 2:
      * Read the exchange rate information.
      * Hint: You will need a mapping between a date/time and rate
      */
    val exchangeRates: DataFrame = getExchangeRates(sqlContext, exchangeRatesPath)

    /**
      * Task 3:
      * UserDefinedFunction to convert between date formats.
      * Hint: Check the formats defined in Constants class
      */
    val convertDate: UserDefinedFunction = sqlContext.udf.register("convertDate", getConvertDate)

    /**
      * Task 3:
      * Transform the rawBids
      * - Convert USD to EUR. The result should be rounded to 3 decimal precision.
      * - Convert dates to proper format - use formats in Constants util class
      * - Get rid of records where there is no price for a Losa or the price is not a proper decimal number
      */
    val bids: DataFrame = getBids(rawBids, exchangeRates)

    /**
      * Task 4:
      * Load motels data.
      * Hint: You will need the motels name for enrichment and you will use the id for join
      */
    val motels: DataFrame = getMotels(sqlContext, motelsPath)

    /**
      * Task5:
      * Join the bids with motel names.
      */
    val enriched: DataFrame = getEnriched(bids, motels)
    enriched.write
      .format(Constants.CSV_FORMAT)
      .save(s"$outputBasePath/$AGGREGATED_DIR")
  }

  def getRawBids(sqlContext: HiveContext, bidsPath: String): DataFrame = {
    sqlContext.read.parquet(bidsPath)
  }

  def getErroneousRecords(rawBids: DataFrame): DataFrame = {
    rawBids.filter(rawBids("HU").contains("ERROR_"))
      .select("BidDate", "HU")
      .groupBy("BidDate", "HU")
      .count()
  }

  def getExchangeRates(sqlContext: HiveContext, exchangeRatesPath: String): DataFrame = {
    sqlContext.read
      .format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .load(exchangeRatesPath)
      .toDF("ValidFrom", "CurrencyName", "CurrencyCode", "ExchangeRate")
  }

  def getConvertDate = (date: String) => {
    Constants.OUTPUT_DATE_FORMAT.print(Constants.INPUT_DATE_FORMAT.parseDateTime(date))
  }

  def getBids(rawBids: DataFrame, exchangeRates: DataFrame): DataFrame = {
    val correctBidsMap = rawBids.filter(!rawBids("HU").contains("ERROR_"))
      .filter(!(rawBids("US").like("") && rawBids("CA").like("") && rawBids("MX").like("")))

    val sourceColumns = correctBidsMap.join(exchangeRates, correctBidsMap.col("BidDate") === exchangeRates.col("ValidFrom"))
      .select("MotelID", "BidDate", "US", "CA", "MX", "ExchangeRate")

    val t = sourceColumns.rdd.map(row => List(
      Row(row.getString(0), row.getString(1), "US", rounded(row.getString(2).toDouble * row.getString(5).toDouble, 3)),
      Row(row.getString(0), row.getString(1), "CA", rounded(row.getString(3).toDouble * row.getString(5).toDouble, 3)),
      Row(row.getString(0), row.getString(1), "MX", rounded(row.getString(4).toDouble * row.getString(5).toDouble, 3))
    ))

    t.count()
    null
  }

  def getMotels(sqlContext: HiveContext, motelsPath: String): DataFrame = ???

  def getEnriched(bids: DataFrame, motels: DataFrame): DataFrame = ???

  def rounded(n: Double, x: Int) = {
    val w = Math.pow(10, x)
    math.round(n * w) / w
  }

  def getBidItem(motelId: String, bidDate: String, loSa: String, price: String, exchangeRate: Double) = {
    Row(motelId, Constants.OUTPUT_DATE_FORMAT.print(Constants.INPUT_DATE_FORMAT.parseDateTime(bidDate)),
      loSa, rounded(if (price.isEmpty) 0 else price.toDouble * exchangeRate, 3))
  }
}
