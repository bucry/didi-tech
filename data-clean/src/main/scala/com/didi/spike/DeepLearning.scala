package com.didi.spike

import org.apache.spark._
import org.apache.spark.sql.SQLContext
//LAB 10 TODO Import classes for MLLib regression labeledpoint, vectors, decisiontree, decisiontree model, MLUtils
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree

object DeepLearning {

  case class Flight(dofM: String, dofW: String, carrier: String, tailnum: String, flnum: Int, org_id: String, origin: String, dest_id: String, dest: String, crsdeptime: Double, deptime: Double, depdelaymins: Double, crsarrtime: Double, arrtime: Double, arrdelay: Double, crselapsedtime: Double, dist: Int)

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[8]").setAppName("DIDI-TECH")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    // function to parse input into Movie class  
    def parseFlight(str: String): Flight = {
      val line = str.split(",")
      println(str)
      Flight(line(0),
        line(1),
        line(2),
        line(3),
        if (line(4) == null) 0 else line(4).toInt,
        line(5),
        line(6),
        line(7),
        line(8),
        if (line(9) == null || line(9).length == 0) 0 else line(9).toDouble,
        if (line(10) == null || line(10).length == 0) 0 else line(10).toDouble,
        if (line(11) == null || line(11).length == 0 ) 0 else line(11).toDouble,
        if (line(12) == null || line(12).length == 0 ) 0 else line(12).toDouble,
        if (line(13) == null || line(13).length == 0) 0 else line(13).toDouble,
        if (line(14) == null || line(14).length == 0) 0 else line(14).toDouble,
        if (line(15) == null || line(15).length == 0) 0 else line(15).toDouble,
        if (line(16) == null || line(16).length == 0) 0 else line(16).toInt
      )
    }

    /* -------------------------------- MLLIB------------------------------------------ */
    //Creating and RDD with the January 2014 data to be used for training the model
    val textRDD = sc.textFile(this.getClass.getResource("rita2014jan.csv").toString)

    val flightsRDD = textRDD.map(parseFlight).cache()

    val flightsDF = flightsRDD.toDF()
    //Register as table
    flightsDF.registerTempTable("flights")

    /*----------------------------ENTRY TO SPARKSQL--------------------------*/
    //creating DF

    //Number of departing flights
    val fltCountsql = sqlContext.sql("select dofM, dest, count(flnum) as total  FROM flights GROUP BY dofM, dest  ORDER BY total DESC LIMIT 5")
    //find average delay
    val avgdepdel = sqlContext.sql("SELECT origin, avg(depdelaymins) as avgdepdelay FROM flights GROUP BY origin ORDER BY avgdepdelay DESC LIMIT 5")

    //  grouped = df[['DepDelayed', 'month']].groupby('month').mean()
    println(s"------------------------------------------------------------------------------------")
    println(s"%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% SPARK SQL - AVERAGE DELAY AT ORIGIN %%%%%%%%%%%%%%%%%%%")
    println(s"-------------------------------------------------------------------------------------------")
    avgdepdel.show()
    fltCountsql.show()
    var carrierMap: Map[String, Int] = Map()
    var index: Int = 0
    flightsRDD.map(flight => flight.carrier).distinct.collect.foreach(x => { carrierMap += (x -> index); index += 1 })

    var originMap: Map[String, Int] = Map()
    var index1: Int = 0
    flightsRDD.map(flight => flight.origin).distinct.collect.foreach(x => { originMap += (x -> index1); index1 += 1 })

    var destMap: Map[String, Int] = Map()
    var index2: Int = 0
    flightsRDD.map(flight => flight.dest).distinct.collect.foreach(x => { destMap += (x -> index2); index2 += 1 })

    //- Defining the features array
    val mlprep = flightsRDD.map(flight => {
      val monthday = flight.dofM.toInt - 1 // category
      val weekday = flight.dofW.toInt - 1 // category
      val crsdeptime1 = flight.crsdeptime.toInt
      val crsarrtime1 = flight.crsarrtime.toInt
      val carrier1 = carrierMap(flight.carrier) // category
      val crselapsedtime1 = flight.crselapsedtime.toDouble
      val origin1 = originMap(flight.origin) // category
      val dest1 = destMap(flight.dest) // category
      val delayed = if (flight.depdelaymins.toDouble > 40) 1.0 else 0.0
      Array(delayed.toDouble, monthday.toDouble, weekday.toDouble, crsdeptime1.toDouble, crsarrtime1.toDouble, carrier1.toDouble, crselapsedtime1.toDouble, origin1.toDouble, dest1.toDouble)
    })
    //Making LabeledPoint of features - this is the training data for the model
    val mldata = mlprep.map(x => LabeledPoint(x(0), Vectors.dense(x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8))))

    val mldata0 = mldata.filter(x => x.label == 0).randomSplit(Array(0.85, 0.15))(1)
    val mldata1 = mldata.filter(x => x.label != 0)
    val mldata2 = mldata0 ++ mldata1
    val splits = mldata2.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    var categoricalFeaturesInfo = Map[Int, Int]()
    categoricalFeaturesInfo += (0 -> 31)
    categoricalFeaturesInfo += (1 -> 7)
    categoricalFeaturesInfo += (4 -> carrierMap.size)
    categoricalFeaturesInfo += (6 -> originMap.size)
    categoricalFeaturesInfo += (7 -> destMap.size)

    val numClasses = 2
    // Defning values for the other parameters
    val impurity = "gini"
    val maxDepth = 9
    val maxBins = 7000

    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      impurity, maxDepth, maxBins)

    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
    println("Test Error = " + testErr)
    
    
  }
}

