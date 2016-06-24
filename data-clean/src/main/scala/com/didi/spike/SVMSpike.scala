package com.didi.spike

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by twcn on 6/24/16.
  */
object SVMSpike {

  def main(args: Array[String]) {
    import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
    import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
    import org.apache.spark.mllib.util.MLUtils

    val conf = new SparkConf().setMaster("local[8]").setAppName("demo")
    val sc = new SparkContext(conf)

    // Load training data in LIBSVM format.
    val data = MLUtils.loadLibSVMFile(sc, this.getClass.getResource("sample_libsvm_data.txt").toString)

    // Split data into training (60%) and test (40%).
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    // Run training algorithm to build the model
    val numIterations = 100
    val model = SVMWithSGD.train(training, numIterations)

    // Clear the default threshold.
    model.clearThreshold()

    // Compute raw scores on the test set.
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()

    println("Area under ROC = " + auROC)

    // Save and load model
    model.save(sc, "myModelPath")
    val sameModel = SVMModel.load(sc, "myModelPath")

    import org.apache.spark.mllib.optimization.L1Updater

    val svmAlg = new SVMWithSGD()
    svmAlg.optimizer.
      setNumIterations(200).
      setRegParam(0.1).
      setUpdater(new L1Updater)
    val modelL1 = svmAlg.run(training)
  }

}
