package twitter

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.log4j.LogManager
import org.apache.log4j.Level

object CountTriangleRSDF {
  
  def main(args: Array[String]) {

    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 3) {
      logger.error("Usage:\ntwitter.CountTriangleRSRDD <input dir> <output dir>")
      System.exit(1)
    }

    val spark = SparkSession
      .builder()
      .appName("Spark Count Triangle")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    val textFile = spark.sparkContext.textFile(args(0))

    val edges = textFile.map(line => (line.split(",")(0), line.split(",")(1)))
                .filter(x => x._1.toLong < args(2).toLong && x._2.toLong < args(2).toLong)
                .toDF("start", "end")

    val path2 = edges.as("df1").join(edges.as("df2")).where($"df1.end" === $"df2.start").drop($"df1.end").drop($"df2.start")
  
    val triangleCount = edges.as("df1").join(path2.as("df2")).where($"df1.end" === $"df2.start" && $"df1.start" === $"df2.end").count()


    logger.info("\nnumber of triangle: \n" + triangleCount / 3)

    spark.stop()

  }
}