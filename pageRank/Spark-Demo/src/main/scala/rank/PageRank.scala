package rank


import org.apache.spark.sql.SparkSession
import org.apache.spark.HashPartitioner
import org.apache.log4j.LogManager
import org.apache.log4j.Level

object PageRank {

  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length < 3) {
      logger.error("Usage:\n spark page rank, should have 3 arguments")
      System.exit(1)
    }

    val spark = SparkSession
      .builder
      .appName("SparkPageRank")
      .getOrCreate()

    val k = args(0).toInt
    val iters = args(1).toInt

    // boardcast the count of page, note its k*k, not k
    val pageCount = spark.sparkContext.broadcast(k * k)


    // create graph with dummy node to handle dangling vertices
    var edgeMap = scala.collection.mutable.Map[Int, Int]()
    for (i <- 1 to k*k) {
      if (i % k == 0) { // dangling vertice, point to dummy node
        edgeMap += (i -> 0)
      }
      else {
        edgeMap += (i -> (i + 1))
      }
    }

    // convert graph to pair RDD
    val graph = spark.sparkContext
      .parallelize(edgeMap.toSeq)
      .distinct()
      .groupByKey()
      .partitionBy(new HashPartitioner(k))
      .persist()

    var ranks = graph.mapValues(v => 1.0)

    // for each iteration
    for (i <- 1 to iters) {
      // use inner join to avoid adding dummy node to the joined graph
      val graphJoined = graph.join(ranks);

      // emit(page, 0) to avoid page with no in-coming links being lost
      val contribPage = graphJoined.map{ case (page, (urls, rank)) => (page, 0.0) }

      // for each page with in-coming links, compute its score
      val contribLinks = graphJoined.flatMap{ case (page, (urls, rank)) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }

      // compute new page rank 
      ranks = (contribPage union contribLinks).reduceByKey(_ + _).map{ case (key, value) =>
        if (key == 0) { // if its dummy node, don't need to consider random jump
          (key, value * 0.85)
        }
        else {
          (key, 0.15 + value * 0.85)
        } 
      }

      // distribute score of dummy node to all other nodes
      val dummyScore = ranks.lookup(0).head / pageCount.value
      ranks = ranks.map{case (key, value) =>
        if (key != 0.0) {
          (key, value + dummyScore)
        }
        else {
          (key, 0.0)
        }
      }

      println("start of lineage after iteration " + i)
      println(ranks.toDebugString)
      println("end of lineage after iteration " + i)
    }

    // print page ranks for each iteration
    //val output1 = ranks.collect()
    val output = ranks.sortByKey().take(101)
    //println("number of iteration: " + i)
    var sum = 0.0
    output.foreach{tup => 
      sum += tup._2.toDouble
      //println(s"${tup._1} has rank:  ${tup._2} ")
    }
    println("sum: " + sum)

    spark.stop()
  }
}


// reference: http://www.ccs.neu.edu/home/mirek/code/SparkPageRank.scala