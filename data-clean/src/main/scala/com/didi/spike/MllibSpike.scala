package com.didi.spike

import com.didi.DiDiSparkContent._

/**
  * Created by twcn on 6/16/16.
  */
object MllibSpike {


  import sqlContext.implicits._



  def validationSplit(): Unit = {
    import org.apache.spark.ml.evaluation.RegressionEvaluator
    import org.apache.spark.ml.regression.LinearRegression
    import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}

    // Prepare training and test data.
    val data = sqlContext.read.format("libsvm").load("data/mllib/sample_linear_regression_data.txt")
    val Array(training, test) = data.randomSplit(Array(0.9, 0.1), seed = 12345)

    val lr = new LinearRegression()

    // We use a ParamGridBuilder to construct a grid of parameters to search over.
    // TrainValidationSplit will try all combinations of values and determine best model using
    // the evaluator.
    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.1, 0.01))
      .addGrid(lr.fitIntercept)
      .addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0))
      .build()

    // In this case the estimator is simply the linear regression.
    // A TrainValidationSplit requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
    val trainValidationSplit = new TrainValidationSplit()
      .setEstimator(lr)
      .setEvaluator(new RegressionEvaluator)
      .setEstimatorParamMaps(paramGrid)
      // 80% of the data will be used for training and the remaining 20% for validation.
      .setTrainRatio(0.8)

    // Run train validation split, and choose the best set of parameters.
    val model = trainValidationSplit.fit(training)

    // Make predictions on test data. model is the model with combination of parameters
    // that performed best.
    model.transform(test)
      .select("features", "label", "prediction")
      .show()
  }


  /**

  16/06/16 11:11:37 INFO BlockManager: Removing RDD 3
+---+---------------+------------------+--------------------+--------------------+--------------------+----------+
| id|           text|             words|            features|       rawPrediction|         probability|prediction|
+---+---------------+------------------+--------------------+--------------------+--------------------+----------+
|  4|    spark i j k|  [spark, i, j, k]|(262144,[105,106,...|[-6.4618140367239...|[0.00155952377334...|       1.0|
|  5|          l m n|         [l, m, n]|(262144,[108,109,...|[12.2075859820562...|[0.99999500758588...|       0.0|
|  6|mapreduce spark|[mapreduce, spark]|(262144,[62173,14...|[-2.7585264898673...|[0.05960690842412...|       1.0|
|  7|  apache hadoop|  [apache, hadoop]|(262144,[128334,1...|[5.24595593272122...|[0.99475883189102...|       0.0|
+---+---------------+------------------+--------------------+--------------------+--------------------+----------+

    **/

  def crossValidation(): Unit = {


    import org.apache.spark.ml.Pipeline
    import org.apache.spark.ml.classification.LogisticRegression
    import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
    import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
    import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}
    import org.apache.spark.mllib.linalg.Vector
    import org.apache.spark.sql.Row

    // Prepare training data from a list of (id, text, label) tuples.
    val training = sqlContext.createDataFrame(Seq(
      (0L, "a b c d e spark", 1.0),
      (1L, "b d", 0.0),
      (2L, "spark f g h", 1.0),
      (3L, "hadoop mapreduce", 0.0),
      (4L, "b spark who", 1.0),
      (5L, "g d a y", 0.0),
      (6L, "spark fly", 1.0),
      (7L, "was mapreduce", 0.0),
      (8L, "e spark program", 1.0),
      (9L, "a e c l", 0.0),
      (10L, "spark compile", 1.0),
      (11L, "hadoop software", 0.0)
    )).toDF("id", "text", "label")

    training.show()

    // Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
    val tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("words")
    val hashingTF = new HashingTF()
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("features")
    val lr = new LogisticRegression()
      .setMaxIter(10).setFeaturesCol("features")
    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, hashingTF, lr))


    // We use a ParamGridBuilder to construct a grid of parameters to search over.
    // With 3 values for hashingTF.numFeatures and 2 values for lr.regParam,
    // this grid will have 3 x 2 = 6 parameter settings for CrossValidator to choose from.
    val paramGrid = new ParamGridBuilder()
      .addGrid(hashingTF.numFeatures, Array(10, 100, 1000))
      .addGrid(lr.regParam, Array(0.1, 0.01))
      .build()

    // We now treat the Pipeline as an Estimator, wrapping it in a CrossValidator instance.
    // This will allow us to jointly choose parameters for all Pipeline stages.
    // A CrossValidator requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
    // Note that the evaluator here is a BinaryClassificationEvaluator and its default metric
    // is areaUnderROC.
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new BinaryClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(2) // Use 3+ in practice

    // Run cross-validation, and choose the best set of parameters.
    val cvModel = cv.fit(training)

    // Prepare test documents, which are unlabeled (id, text) tuples.
    val test = sqlContext.createDataFrame(Seq(
      (4L, "spark i j k"),
      (5L, "l m n"),
      (6L, "mapreduce spark"),
      (7L, "apache hadoop")
    )).toDF("id", "text")

    // Make predictions on test documents. cvModel uses the best model found (lrModel).
    cvModel.transform(test)
      .select("id", "text", "probability", "prediction")
      .collect()
      .foreach { case Row(id: Long, text: String, prob: Vector, prediction: Double) =>
        println(s"($id, $text) --> prob=$prob, prediction=$prediction")
      }
  }


  def kMeans() = {
    import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
    import org.apache.spark.mllib.linalg.Vectors

    // Load and parse the data
    val data = sc.textFile(this.getClass.getResource("kmeans_data.txt").toString)
    val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()

    // Cluster the data into two classes using KMeans
    val numClusters = 2
    val numIterations = 20
    val clusters = KMeans.train(parsedData, numClusters, numIterations)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = " + WSSSE)


    val ss=parsedData.map(v => v.toString+ " belong to cluster :" + clusters.predict(v)).collect
    ss.foreach(a=>println(a))

    // Save and load model

    //TODO you need delete old model
    clusters.save(sc, "myModelPath")
    val sameModel = KMeansModel.load(sc, "myModelPath")


    println(sameModel.predict(Vectors.dense(Array(9.2, 9.2, 9.2))))
    println("------")

    println(sameModel.predict(parsedData).toDF().show())
  }

}
