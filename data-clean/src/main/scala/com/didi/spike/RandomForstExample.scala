package com.didi.spike

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.tree.configuration
import org.apache.spark.mllib.tree.configuration.Strategy
object RandomForstExample {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("RandomForestExample").
      setMaster("spark://sparkmaster:7077")
    val sc = new SparkContext(sparkConf)

    val data: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc, "/data/sample_data.txt")

    val numClasses = 2
    val featureSubsetStrategy = "auto"
    val numTrees = 3
//    val model: RandomForestModel =RandomForest.trainClassifier(
//      data, new Strategy().defaultStrategy("classification"),numTrees,
//      featureSubsetStrategy,new java.util.Random().nextInt())

    val model: RandomForestModel =RandomForest.trainClassifier(
      data, null,numTrees,
      featureSubsetStrategy,new java.util.Random().nextInt())

    val input: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc, "/data/input.txt")

    val predictResult = input.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    //打印输出结果，在 spark-shell 上执行时使用
    predictResult.collect()
    //将结果保存到 hdfs //predictResult.saveAsTextFile("/data/predictResult")
    sc.stop()

  }
}