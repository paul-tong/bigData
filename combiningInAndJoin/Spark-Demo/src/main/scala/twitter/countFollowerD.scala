package twitter

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.log4j.LogManager
import org.apache.log4j.Level

object CountFollowerD {
  
  def main(args: Array[String]) {

    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\ntwitter.CountFollwerD <input dir> <output dir>")
      System.exit(1)
    }

    val spark = SparkSession
      .builder()
      .appName("Spark Count Follower")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    val textFile = spark.sparkContext.textFile(args(0))

    val countsDF = textFile.map(line => line.split(",")(1))
                 .map(id => (id, 1))
                 .toDF("id", "count")
                 .groupBy("id")
                 .sum()

    countsDF.explain(true)
    countsDF.write.csv(args(1))

    //counts.show()

    //val countsRDD = counts.rdd.map(_.toString())
    //countsRDD.show()

    //countsRDD.saveAsTextFile(args(1))

    //counts.rdd.map(_.toString()).saveAsTextFile(args(1))
    // counts.write.repartition(1).format("com.databricks.spark.csv").save(args(0))
    //counts.saveAsTextFile(args(1))

    spark.stop()
  }
}