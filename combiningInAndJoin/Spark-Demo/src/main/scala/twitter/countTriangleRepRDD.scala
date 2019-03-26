package twitter

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.log4j.LogManager
import org.apache.log4j.Level

object CountTriangleRepRDD {
  
  def main(args: Array[String]) {

    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 3) {
      logger.error("Usage:\ntwitter.CountFollwerD <input dir> <output dir>")
      System.exit(1)
    }    

    val spark = SparkSession
      .builder()
      .appName("Spark Count Triangle")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    // global counter
    val triangleCount = spark.sparkContext.accumulator(0);

    val textFile = spark.sparkContext.textFile(args(0))

    val edges = textFile.map(line => (line.split(",")(0), line.split(",")(1)))
                .filter(x => x._1.toLong < args(2).toLong && x._2.toLong < args(2).toLong)


    // build a multiMap <start, set<end>>, key is start point, value is a set of end points
    val mmap = new collection.mutable.HashMap[String, collection.mutable.Set[String]]() with collection.mutable.MultiMap[String, String]

    edges.collect
        .map(edge => { 
          mmap.addBinding(edge._1, edge._2) 
        })

    val edgesMap = spark.sparkContext.broadcast(mmap)
    //    val edgesMap = spark.sparkContext.broadcast(edges.collectAsMap)
    //broadcast( smallRDD.collect.toMap)

    val path2 = edges.flatMap{case(start, end) => edgesMap.value.get(end).map {path2Ends => (start, path2Ends)}}


    edges.foreach{
      case(start, end) =>  // for each edge
      if (edgesMap.value.contains(end)) { // if end of this edge is in the map
        edgesMap.value.get(end).foreach { // get each end of path2
          (path2Ends) => 
            path2Ends.foreach{
            (path2End) => 
              edgesMap.value.get(path2End).foreach { // check if there is an edge from path2end => start (close the path2)
                (path2Closes) => 
                  if (path2Closes.contains(start)) {
                    triangleCount += 1
                  }
             }
          }
        }
      }
    }

    logger.info("\nnumber of triangle: \n" + triangleCount.value / 3)

    spark.stop()
  }
}