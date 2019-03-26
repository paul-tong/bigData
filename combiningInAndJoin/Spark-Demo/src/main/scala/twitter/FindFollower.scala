package twitter

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level

object FindFollower {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\ntwitter.FindFollwer <input dir> <output dir>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Find Follower")
    val sc = new SparkContext(conf)    
    val textFile = sc.textFile(args(0))

    /* Get the id of person being followed. Note cannot use 
       textFile.flatMap, because it will split id into digits.
    */
    val counts = textFile.map(line => line.split(",")(1))
                 .map(id => (id, 1))
                 .reduceByKey(_ + _)

    counts.saveAsTextFile(args(1))
  }
}
