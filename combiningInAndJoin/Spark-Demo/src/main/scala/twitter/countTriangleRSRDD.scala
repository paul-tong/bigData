package twitter

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.log4j.LogManager
import org.apache.log4j.Level

object CountTriangleRSRDD {
  
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

    val reversedEdges = edges.map(edge => (edge._2, edge._1))

    val path2 = edges.join(reversedEdges).map(path => path._2)

    val triangleCount = edges.join(path2)
                    .map(triangle => triangle._2)
                    .filter(x => x._1 == x._2)
                    .count()

    logger.info("\nnumber of triangle: \n" + triangleCount / 3)

    spark.stop()

  }
}