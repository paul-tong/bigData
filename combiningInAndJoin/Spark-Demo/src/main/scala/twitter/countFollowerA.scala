package twitter

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level

object CountFollowerA {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\ntwitter.CountFollwer <input dir> <output dir>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Count Follower")
    val sc = new SparkContext(conf)    
    val textFile = sc.textFile(args(0))

    val counts = textFile.map(line => line.split(",")(1))
                 .map(id => (id, 1))
                 .aggregateByKey(0)(_+_,_+_)

    println("Debug String Begin:")
    logger.info(counts.toDebugString)
    println("Debug String End")
    
    counts.saveAsTextFile(args(1))
  }
}
