package com.didi.spike

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by twcn on 6/22/16.
  */
object SparkML {


  def main(args: Array[String]) {
    import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
    import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
    import org.apache.spark.mllib.util.MLUtils
    import org.apache.spark.sql.Row

    val conf = new SparkConf().setMaster("local[8]").setAppName("DIDI-TECH")
    val sc = new SparkContext(conf)
    val sqlCtx = new SQLContext(sc)

    // Load training data
    val data = sqlCtx.createDataFrame(MLUtils.loadLibSVMFile(sc, this.getClass.getResource("sample_multiclass_classification_data.txt").toString))
    // Split the data into train and test

    data.show()
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 1234L)
    val train = splits(0)
    val test = splits(1)
    // specify layers for the neural network:
    // input layer of size 4 (features), two intermediate of size 5 and 4 and output of size 3 (classes)
    val layers = Array[Int](4, 5, 4, 3)
    // create the trainer and set its parameters
    val trainer = new MultilayerPerceptronClassifier()
      .setLayers(layers)
      .setBlockSize(128)
      .setSeed(1234L)
      .setMaxIter(100)
    // train the model
    val model = trainer.fit(train)
    // compute precision on the test set
    val result = model.transform(test)
    val predictionAndLabels = result.select("prediction", "label")

    predictionAndLabels.show()

    val evaluator = new MulticlassClassificationEvaluator()
      .setMetricName("precision")
    println("Precision:" + evaluator.evaluate(predictionAndLabels))
  }

}
