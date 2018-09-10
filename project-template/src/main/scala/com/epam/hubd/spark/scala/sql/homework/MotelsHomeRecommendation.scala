package com.epam.hubd.spark.scala.sql.homework

import org.apache.spark.sql.{DataFrame, _}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.{SparkConf, SparkContext}

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

    processData(sqlContext, bidsPath, motelsPath, exchangeRatesPath, outputBasePath)

    sc.stop()
  }

  def processData(sqlContext: HiveContext, bidsPath: String, motelsPath: String, exchangeRatesPath: String, outputBasePath: String) = {

    /**
      * Task 1:
      * Read the bid data from the provided file.
      * Split each line from the file and return list of strings.
      * Example:
      * input -> "0000002,15-04-08-2016,0.89,0.92,1.32,2.07,1.35"
      * output -> Row("0000002", "15-04-08-2016", "0.89", "0.92", "1.32", "2.07", "1.35")
      */
    val rawBids: DataFrame = getRawBids(sqlContext, bidsPath)
    rawBids.createOrReplaceTempView("raw_bids")

    /**
      * Task 1:
      * Collect the errors and save the result.
      * Find all records which contain ERROR message and after that group them by date.
      * Example:
      * input ->  Row("1", "06-05-02-2016", "ERROR_1"),
      *           Row("3", "07-05-02-2016", "ERROR_2"),
      *           Row("4", "06-05-02-2016", "ERROR_1")
      * output -> Row("06-05-02-2016","ERROR_1","2"),
      *           Row("07-05-02-2016","ERROR_2","1")
      */
    val erroneousRecords: DataFrame = getErroneousRecords(rawBids)
    erroneousRecords.write
      .format(Constants.CSV_FORMAT)
      .save(s"$outputBasePath/$ERRONEOUS_DIR")

    /**
      * Task 2:
      * Read the exchange rate information.
      * Split each line and return DataFrame. Each row contains information about one exchange rate.
      * Example:
      * input -> "11-05-08-2016,Euro,EUR,0.873","10-06-11-2015,Euro,EUR,0.987","10-05-02-2016,Euro,EUR,0.876"
      * output -> Row("11-05-08-2016","Euro","EUR","0.873"),
      *           Row("10-06-11-2015","Euro","EUR","0.987"),
      *           Row("10-05-02-2016","Euro","EUR","0.876")
      */
    val exchangeRates: DataFrame = getExchangeRates(sqlContext, exchangeRatesPath)
    exchangeRates.createOrReplaceTempView("exchange_rates")

    /**
      * Task 3:
      * Create UserDefinedFunction to convert between date formats.
      * Register this UserDefinedFunction by the "convertDate" name.
      */
    val convertDate: UserDefinedFunction = sqlContext.udf.register("convertDate", getConvertDate(_: String))

    /**
      * Task 4:
      * Transform the rawBids and use the BidItem case class.
      * Get all valid records, convert each record to three records based on the interested countries,
      * and return all records with the highest price.
      * Example:
      * input rawBids -> Row("0000002", "11-05-08-2016", "0.92", "1.68", "0.81", "0.68", "1.59", "", "1.63", "1.77", "2.06", "0.66", "1.53", "", "0.32", "0.88", "0.83", "1.01")
      * input exchangeRates ->  Row("11-05-08-2016","Euro","EUR","0.873"),
      *                         Row("10-06-11-2015","Euro","EUR","0.987"),
      *                         Row("10-05-02-2016","Euro","EUR","0.876")
      * output -> Row("0000002", "2016-08-05 11:00", "CA", 1.423)
      */
    val bids: DataFrame = getBids(rawBids, exchangeRates, sqlContext)

    /**
      * Task 5:
      * Load motels data.
      * Split each line and return DataFrame. Each row of the DataFrame represents the information about one of the motels.
      * Example:
      * input ->  "0000001,Olinda Windsor Inn,IN,http://www.motels.home/?partnerID=3cc6e91b-a2e0-4df4-b41c-766679c3fa28,Description",
      *           "0000002,Merlin Por Motel,JP,http://www.motels.home/?partnerID=4ba51050-eff2-458a-93dd-6360c9d94b63,Description"
      * output -> Row("0000001", "Olinda Windsor Inn","IN","http://www.motels.home/?partnerID=3cc6e91b-a2e0-4df4-b41c-766679c3fa28", "Description"),
      *           Row("0000002", "Merlin Por Motel", "JP", "http://www.motels.home/?partnerID=4ba51050-eff2-458a-93dd-6360c9d94b63", "Description")
      */
    val motels: DataFrame = getMotels(sqlContext, motelsPath)

    /**
      * Task 6:
      * Join the bids with motel names and create a new DataFrame. Each row of the DataFrame contains information about bid and motel name.
      * Example:
      * input bids -> Row("0000002", "2016-08-05 11:00", "CA", 1.423), Row("0000003", "2015-11-07 04:00", "MX", 0.994)
      * input motels -> Row("0000001", "Olinda Windsor Inn","IN","http://www.motels.home/?partnerID=3cc6e91b-a2e0-4df4-b41c-766679c3fa28", "Description"),
      *                 Row("0000002", "Merlin Por Motel", "JP", "http://www.motels.home/?partnerID=4ba51050-eff2-458a-93dd-6360c9d94b63", "Description"),
      *                 Row("0000003", "Olinda Big River Casino", "JP", "http://www.motels.home/?partnerID=4ba51050-eff2-458a-93dd-6360c9d94b63", "Description")
      * output -> Row("0000002", "Merlin Por Motel", "2016-08-05 11:00", "CA", 1.423),
      *           Row("0000003", "Olinda Big River Casino", "2015-11-07 04:00", "MX", 0.994)
      */
    val enriched: DataFrame = getEnriched(bids, motels)
    enriched.write
      .format(Constants.CSV_FORMAT)
      .save(s"$outputBasePath/$AGGREGATED_DIR")
  }

  def getRawBids(sqlContext: HiveContext, bidsPath: String): DataFrame = {
    sqlContext.read
      .parquet(bidsPath)
      .toDF("MotelID", "BidDate", "HU", "UK",  "NL", "US", "MX", "AU", "CA", "CN", "KR","BE", "I","JP", "IN", "HN", "GY", "DE")
  }

  def getErroneousRecords(rawBids: DataFrame): DataFrame = {
    rawBids.filter(rawBids("HU").contains("ERROR_"))
      .select("BidDate", "HU")
      .groupBy("BidDate", "HU")
      .count()
  }

  def getExchangeRates(sqlContext: HiveContext, exchangeRatesPath: String): DataFrame = {
    sqlContext.read
      .format(Constants.CSV_FORMAT)
      .option("delimiter", Constants.DELIMITER)
      .load(exchangeRatesPath)
      .toDF("ValidFrom", "CurrencyName", "CurrencyCode", "ExchangeRate")
  }

  def getConvertDate = (date: String) => {
    Constants.OUTPUT_DATE_FORMAT.print(Constants.INPUT_DATE_FORMAT.parseDateTime(date))
  }

  def getBids(rawBids: DataFrame, exchangeRates: DataFrame, sqlContext: SQLContext): DataFrame = {
    import sqlContext.implicits._
    val correctBidsMap = rawBids.filter(!rawBids("HU").contains("ERROR_"))
      .filter(!(rawBids("US").like("") && rawBids("CA").like("") && rawBids("MX").like("")))

    correctBidsMap.join(exchangeRates, correctBidsMap.col("BidDate") === exchangeRates.col("ValidFrom"))
      .select("MotelID", "BidDate", "US", "CA", "MX", "ExchangeRate")
      .flatMap(row => {
        val tempList = List(
          (row.getString(0), row.getString(1), "US", rounded(getNotEmptyPrice(row.getString(2), row.getString(5)), 3)),
          (row.getString(0), row.getString(1), "CA", rounded(getNotEmptyPrice(row.getString(3), row.getString(5)), 3)),
          (row.getString(0), row.getString(1), "MX", rounded(getNotEmptyPrice(row.getString(4), row.getString(5)), 3))
        )
        val maxExchangeRate = tempList.maxBy(row => row._4)
        tempList.filter(row => row._4 == maxExchangeRate._4)
      }).toDF("MotelID", "BidDate", "Losa", "ExchangeRate")
      .createOrReplaceTempView("total_price")

    sqlContext.sql("SELECT MotelID, convertDate(BidDate) AS BidDate, Losa, ExchangeRate FROM total_price")
  }

  def getMotels(sqlContext: HiveContext, motelsPath: String): DataFrame = {
    sqlContext.read
      .parquet(motelsPath)
      .toDF("Motel_ID", "MotelName", "Country", "URL", "Comment")
  }

  def getEnriched(bids: DataFrame, motels: DataFrame): DataFrame = {
    bids.join(motels, bids.col("MotelID") === motels.col("Motel_ID"))
      .select("MotelID", "MotelName", "BidDate", "Losa", "ExchangeRate")
  }

  def rounded(n: Double, x: Int) = {
    val w = Math.pow(10, x)
    math.round(n * w) / w
  }

  def getNotEmptyPrice(firstPrice: String, exchangePrice: String) = {
    if (firstPrice.isEmpty) 0.0 else firstPrice.toDouble * exchangePrice.toDouble
  }
}
