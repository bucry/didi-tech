package com.didi.spike

import com.didi.DiDiSparkContent._

/**
  * Created by twcn on 6/16/16.
  */
object UnevenSpike {

  import sqlContext.implicits._

  def svm() = {
    import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
    import org.apache.spark.mllib.evaluation.MulticlassMetrics
    import org.apache.spark.mllib.regression.LabeledPoint
    import org.apache.spark.mllib.util.MLUtils

    // Load training data in LIBSVM format
    val data = MLUtils.loadLibSVMFile(sc, this.getClass.getResource("featureTranformer.csv").toString)

    // Split data into training (60%) and test (40%)
    val Array(training, test) = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    training.cache()

    // Run training algorithm to build the model
    val model = new LogisticRegressionWithLBFGS()
      .setNumClasses(3)
      .run(training)

    // Compute raw scores on the test set
    val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }

    // Instantiate metrics object
    val metrics = new MulticlassMetrics(predictionAndLabels)

    // Confusion matrix
    println("Confusion matrix:")
    println(metrics.confusionMatrix)

    // Overall Statistics
    val precision = metrics.precision
    val recall = metrics.recall // same as true positive rate
    val f1Score = metrics.fMeasure
    println("Summary Statistics")
    println(s"Precision = $precision")
    println(s"Recall = $recall")
    println(s"F1 Score = $f1Score")

    // Precision by label
    val labels = metrics.labels
    labels.foreach { l =>
      println(s"Precision($l) = " + metrics.precision(l))
    }

    // Recall by label
    labels.foreach { l =>
      println(s"Recall($l) = " + metrics.recall(l))
    }

    // False positive rate by label
    labels.foreach { l =>
      println(s"FPR($l) = " + metrics.falsePositiveRate(l))
    }

    // F-measure by label
    labels.foreach { l =>
      println(s"F1-Score($l) = " + metrics.fMeasure(l))
    }

    // Weighted stats
    println(s"Weighted precision: ${metrics.weightedPrecision}")
    println(s"Weighted recall: ${metrics.weightedRecall}")
    println(s"Weighted F1 score: ${metrics.weightedFMeasure}")
    println(s"Weighted false positive rate: ${metrics.weightedFalsePositiveRate}")
  }



  def randomForests() = {
    import org.apache.spark.mllib.tree.RandomForest
    import org.apache.spark.mllib.tree.model.RandomForestModel
    import org.apache.spark.mllib.util.MLUtils

    // Load and parse the data file.
    val data = MLUtils.loadLibSVMFile(sc, this.getClass.getResource("sample_libsvm_data.txt").toString)
    // Split the data into training and test sets (30% held out for testing)
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    // Train a RandomForest model.
    // Empty categoricalFeaturesInfo indicates all features are continuous.
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numTrees = 3 // Use more in practice.
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity = "gini"
    val maxDepth = 4
    val maxBins = 32

    val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

    // Evaluate model on test instances and compute test error
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
    println("Test Error = " + testErr)
    println("Learned classification forest model:\n" + model.toDebugString)

    // Save and load model
    model.save(sc, "target/tmp/myRandomForestClassificationModel")
    val sameModel = RandomForestModel.load(sc, "target/tmp/myRandomForestClassificationModel")
  }
}
