To run the application, you need to execute the following command:
```
    spark-submit --class com.epam.hubd.spark.scala.sql.homework.MotelsHomeRecommendation spark-core-homework-1.0.0.jar integration/input/bids.gz.parquet integration/input/motels.gz.parquet integration/input/exchange_rate.txt /tmp/spark-sql
```
You must ensure that the output directory does not exist and that the output folder is in the correct location.

 