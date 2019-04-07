package classification

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}
 
object Tree {
 
  def main(args: Array[String]) {
 
    val conf = new SparkConf().setAppName("DecisionTree").setMaster("local")
    val sc = new SparkContext(conf)
    Logger.getRootLogger.setLevel(Level.WARN)

    // read input file and split it into training and test data
    val data = sc.textFile(args(0)) 
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (data1, data2) = (splits(0), splits(1))


    // convert each line to lable and attributes vector
    val tree1 = data1.map { line =>
      val parts = line.split(',')
      LabeledPoint(parts(1).toDouble, Vectors.dense(parts(2).split(' ').map(_.toDouble)))
    }
 
    val tree2 = data2.map { line =>
      val parts = line.split(',')
      LabeledPoint(parts(1).toDouble, Vectors.dense(parts(2).split(' ').map(_.toDouble)))
    }
 
    val (trainingData, testData) = (tree1, tree2)
 
    // set decision tree parameters
    val numClasses = 3
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"
    val maxDepth = 5 
    val maxBins = 32 // max width
 
    // train the model
    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      impurity, maxDepth, maxBins)
 
    // prediction with model
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
 
    // compare predict result with actual result
    val print_predict = labelAndPreds.take(15)
    println("label" + "\t" + "prediction")
    for (i <- 0 to print_predict.length - 1) {
      println(print_predict(i)._1 + "\t" + print_predict(i)._2)
    }
 
    labelAndPreds.saveAsTextFile(args(1))

    // print error percentage
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
    println("Test Error = " + testErr)

    // print model to tree
    println("Learned classification tree model:\n" + model.toDebugString)
 
    // save model to file
    model.save(sc, args(2))
  }
}