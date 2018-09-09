package com.epam.hubd.spark.scala.sql.homework

import java.io.File

import com.epam.hubd.spark.scala.core.util.RddComparator
import com.epam.hubd.spark.scala.sql.homework.MotelsHomeRecommendation.{AGGREGATED_DIR, ERRONEOUS_DIR, getConvertDate}
import com.epam.hubd.spark.scala.sql.homework.MotelsHomeRecommendationTest._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit._
import org.junit.rules.TemporaryFolder

/**
  * Created by Csaba_Bejan on 8/22/2016.
  */
class MotelsHomeRecommendationTest {
  val _temporaryFolder = new TemporaryFolder

  @Rule
  def temporaryFolder = _temporaryFolder

  val INPUT_BIDS_SAMPLE = "src/test/resources/bids_sample.txt"
  val INPUT_EXCHANGE_RATE_SAMPLE = "src/test/resources/exchange_rate_sample.txt"
  val INPUT_MOTELS_SAMPLE = "src/test/resources/motels_sample.txt"

  val INPUT_BIDS_INTEGRATION = "src/test/resources/integration/input/bids.gz.parquet"
  val INPUT_EXCHANGE_RATES_INTEGRATION = "src/test/resources/integration/input/exchange_rate.txt"
  val INPUT_MOTELS_INTEGRATION = "src/test/resources/integration/input/motels.gz.parquet"

  val EXPECTED_AGGREGATED_INTEGRATION = "src/test/resources/integration/expected_output/aggregated"
  val EXPECTED_ERRORS_INTEGRATION = "src/test/resources/integration/expected_output/expected_sql"

  val RAW_BIDS_SCHEMA = List(
    StructField("MotelID", StringType, true),
    StructField("BidDate", StringType, true),
    StructField("HU", StringType, true),
    StructField("UK", StringType, true),
    StructField("NL", StringType, true),
    StructField("US", StringType, true),
    StructField("MX", StringType, true),
    StructField("AU", StringType, true),
    StructField("CA", StringType, true),
    StructField("CN", StringType, true),
    StructField("KR", StringType, true),
    StructField("BE", StringType, true),
    StructField("I", StringType, true),
    StructField("JP", StringType, true),
    StructField("IN", StringType, true),
    StructField("HN", StringType, true),
    StructField("GY", StringType, true),
    StructField("DE", StringType, true)
  )

  val EXCHANGE_RATES_SCHEMA = List(
    StructField("ValidFrom", StringType, true),
    StructField("CurrencyName", StringType, true),
    StructField("CurrencyCode", StringType, true),
    StructField("ExchangeRate", StringType, true)
  )

  val BIDS_SCHEMA = List(
    StructField("MotelID", StringType, true),
    StructField("BidDate", StringType, true),
    StructField("Losa", StringType, true),
    StructField("ExchangeRate", DoubleType, false)
  )

  val MOTELS_SCHEMA = List(
    StructField("Motel_ID", StringType, true),
    StructField("MotelName", StringType, true),
    StructField("Country", StringType, true),
    StructField("URL", StringType, true),
    StructField("Comment", StringType, true)
  )

  private var outputFolder: File = null

  @Before
  def setup() = {
    outputFolder = temporaryFolder.newFolder("output")
  }

  @Test
  def shouldFilterErrorsAndCreateCorrectAggregates() = {

    runIntegrationTest()

    assertRddTextFiles(EXPECTED_ERRORS_INTEGRATION, getOutputPath(ERRONEOUS_DIR))
    assertRddTextFiles(EXPECTED_AGGREGATED_INTEGRATION, getOutputPath(AGGREGATED_DIR))
  }

  @Test
  def shouldReadFileWithExchangeRates() = {
    val expectedSchema = List(
      StructField("ValidFrom", StringType, true),
      StructField("CurrencyName", StringType, true),
      StructField("CurrencyCode", StringType, true),
      StructField("ExchangeRate", StringType, true)
    )

    val expectedData = Seq(
      Row("11-05-08-2016", "Euro", "EUR", "0.873"),
      Row("10-06-11-2015", "Euro", "EUR", "0.987"),
      Row("10-05-02-2016", "Euro", "EUR", "0.876")
    )

    val expectedDF = sqlContext.createDataFrame(
      sqlContext.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    )

    val actual = MotelsHomeRecommendation.getExchangeRates(sqlContext, INPUT_EXCHANGE_RATE_SAMPLE)
    Assert.assertTrue(assertDataFrameEquals(expectedDF, actual, true))
  }

  @Test
  def shouldReturnBidsSplittedByCountries() = {
    sqlContext.udf.register("convertDate", getConvertDate(_: String))

    val rawBidsData = Seq(
      Row("0000002", "11-05-08-2016", "0.92", "1.68", "0.81", "0.68", "1.59", "", "1.63", "1.77", "2.06", "0.66", "1.53", "", "0.32", "0.88", "0.83", "1.01"),
      Row("0000002", "10-06-11-2015", "0.92", "1.68", "0.81", "0.79", "1.21", "1.44", "1.04", "1.77", "2.06", "0.66", "1.53", "", "0.32", "0.88", "0.83", "1.01")
    )

    val rawBidsDF = sqlContext.createDataFrame(
      sqlContext.sparkContext.parallelize(rawBidsData),
      StructType(RAW_BIDS_SCHEMA)
    )

    val exchangeRatesData = Seq(
      Row("11-05-08-2016", "Euro", "EUR", "0.873"),
      Row("10-06-11-2015", "Euro", "EUR", "0.987"),
      Row("10-05-02-2016", "Euro", "EUR", "0.876")
    )

    val exchangeRatesDF = sqlContext.createDataFrame(
      sqlContext.sparkContext.parallelize(exchangeRatesData),
      StructType(EXCHANGE_RATES_SCHEMA)
    )

    val expectedData = Seq(
      Row("0000002", "2015-11-06 10:00", "MX", 1.194),
      Row("0000002", "2016-08-05 11:00", "CA", 1.423)
    )

    val expectedDF = sqlContext.createDataFrame(
      sqlContext.sparkContext.parallelize(expectedData),
      StructType(BIDS_SCHEMA)
    )

    val actual = MotelsHomeRecommendation.getBids(rawBidsDF, exchangeRatesDF, sqlContext)
    Assert.assertTrue(assertDataFrameEquals(expectedDF, actual, true))
  }

  @Test
  def shouldReturnBidsWithMotelName() = {
    val bidsData = Seq(
      Row("0000002", "2016-08-05 11:00", "CA", 1.423),
      Row("0000005", "2016-09-04 18:00", "US", 1.154),
      Row("0000003", "2015-11-07 04:00", "MX", 0.994)
    )

    val bidsDF = sqlContext.createDataFrame(
      sqlContext.sparkContext.parallelize(bidsData),
      StructType(BIDS_SCHEMA)
    )

    val motelsData = Seq(
      Row("0000001", "Olinda Windsor Inn", "IN", "http://www.motels.home/?partnerID=3cc6e91b-a2e0-4df4-b41c-766679c3fa28", "Test"),
      Row("0000002", "Merlin Por Motel", "JP", "http://www.motels.home/?partnerID=4ba51050-eff2-458a-93dd-6360c9d94b63", "Test2"),
      Row("0000003", "Olinda Big River Casino", "JP", "http://www.motels.home/?partnerID=4ba51050-eff2-458a-93dd-6360c9d94b63", "Test2"),
      Row("0000004", "Majestic Big River Elegance Plaza", "JP", "http://www.motels.home/?partnerID=4ba51050-eff2-458a-93dd-6360c9d94b63", "Test2"),
      Row("0000005", "Majestic Ibiza Por Hostel", "JP", "http://www.motels.home/?partnerID=4ba51050-eff2-458a-93dd-6360c9d94b63", "Test2")
    )

    val motelsDF = sqlContext.createDataFrame(
      sqlContext.sparkContext.parallelize(motelsData),
      StructType(MOTELS_SCHEMA)
    )

    val expectedSchema = List(
      StructField("MotelID", StringType, true),
      StructField("MotelName", StringType, true),
      StructField("BidDate", StringType, true),
      StructField("Losa", StringType, true),
      StructField("ExchangeRate", DoubleType, false)
    )

    val expectedData = Seq(
      Row("0000002", "Merlin Por Motel", "2016-08-05 11:00", "CA", 1.423),
      Row("0000005", "Majestic Ibiza Por Hostel", "2016-09-04 18:00", "US", 1.154),
      Row("0000003", "Olinda Big River Casino", "2015-11-07 04:00", "MX", 0.994)
    )

    val expectedDF = sqlContext.createDataFrame(
      sqlContext.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    )

    val actual = MotelsHomeRecommendation.getEnriched(bidsDF, motelsDF)
    Assert.assertTrue(assertDataFrameEquals(expectedDF, actual, true))
  }

  @After
  def teardown(): Unit = {
    outputFolder.delete
  }

  private def runIntegrationTest() = {
    MotelsHomeRecommendation.processData(sqlContext, INPUT_BIDS_INTEGRATION, INPUT_MOTELS_INTEGRATION, INPUT_EXCHANGE_RATES_INTEGRATION, outputFolder.getAbsolutePath)
  }

  private def assertRddTextFiles(expectedPath: String, actualPath: String) = {
    val expected = sc.textFile(expectedPath)
    val actual = sc.textFile(actualPath)
    RddComparator.printDiff(expected, actual)
  }

  private def assertDataFrameEquals(a: DataFrame, b: DataFrame, isRelaxed: Boolean): Boolean = {
    try {

      a.rdd.cache
      b.rdd.cache

      // 1. Check the equality of two schemas
      if (!a.schema.toString().equalsIgnoreCase(b.schema.toString)) {
        return false
      }

      // 2. Check the number of rows in two dfs
      if (a.count() != b.count()) {
        return false
      }

      // 3. Check there is no unequal rows
      val aColumns: Array[String] = a.columns
      val bColumns: Array[String] = b.columns

      // To correctly handles cases where the DataFrames may have columns in different orders
      scala.util.Sorting.quickSort(aColumns)
      scala.util.Sorting.quickSort(aColumns)
      val aSeq: Seq[Column] = aColumns.map(col(_))
      val bSeq: Seq[Column] = bColumns.map(col(_))

      var a_prime: DataFrame = null
      var b_prime: DataFrame = null

      if (isRelaxed) {
        a_prime = a
        //            a_prime.show()
        b_prime = b
        //            a_prime.show()
      }
      else {
        // To correctly handles cases where the DataFrames may have duplicate rows and/or rows in different orders
        a_prime = a.sort(aSeq: _*).groupBy(aSeq: _*).count()
        //    a_prime.show()
        b_prime = b.sort(aSeq: _*).groupBy(bSeq: _*).count()
        //    a_prime.show()
      }

      val c1: Long = a_prime.except(b_prime).count()
      val c2: Long = b_prime.except(a_prime).count()

      if (c1 != c2 || c1 != 0 || c2 != 0) {
        return false
      }
    } finally {
      a.rdd.unpersist()
      b.rdd.unpersist()
    }

    true
  }

  private def getOutputPath(dir: String): String = {
    new Path(outputFolder.getAbsolutePath, dir).toString
  }
}

object MotelsHomeRecommendationTest {
  var sc: SparkContext = null
  var sqlContext: HiveContext = null

  @BeforeClass
  def beforeTests() = {
    sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("motels-home-recommendation test"))
    sqlContext = new HiveContext(sc)
  }

  @AfterClass
  def afterTests() = {
    sc.stop
  }
}
