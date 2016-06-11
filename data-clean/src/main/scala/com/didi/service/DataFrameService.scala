package com.didi.service

import com.didi.DiDiSparkContent._
import com.didi.config.TsvFilePath
import com.didi.models._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame


object DataFrameService {

  import sqlContext.implicits._

  def loadOrderDataFrame(): DataFrame = {
    val orderRDD = loadRDD(sc, TsvFilePath.orderFilePath)
    val orderDataFrame = orderRDD.map(f => Order(f(0), f(1), f(2), f(3), f(4), f(5).trim.toDouble, f(6))).toDF()
    orderDataFrame.registerTempTable("order")
    orderDataFrame
  }

  def loadTrafficDataFrame(): DataFrame = {
    val trafficRDD = loadRDD(sc, TsvFilePath.trafficFilePath)
    val trafficDataFrame = trafficRDD.map(f => Traffic(f(0), f(1), f(2))).toDF()
    trafficDataFrame.registerTempTable("traffic")
    trafficDataFrame
  }


  def loadPoiDataFrame(): DataFrame = {
    val poiRDD = loadRDD(sc, TsvFilePath.poiFilePath)
    val poiDataFrame = poiRDD.map(f => Poi(f(0), f(1))).toDF()
    poiDataFrame.registerTempTable("poi")
    poiDataFrame
  }

  def loadClusterDataFrame(): DataFrame = {
    val clusterRDD = loadRDD(sc, TsvFilePath.clusterFilePath)
    val clusterDataFrame = clusterRDD.map(f => Cluster(f(0), f(1))).toDF()
    clusterDataFrame.registerTempTable("cluster")
    clusterDataFrame
  }

  def loadWeatherDataFrame(): DataFrame = {
    val weatherRDD = loadRDD(sc, TsvFilePath.weatherFilePath)
    val weatherDataFrame = weatherRDD.map(f => Weather(f(0), f(1).trim.toInt, f(2).trim.toDouble, f(3).trim.toDouble)).toDF()
    weatherDataFrame.registerTempTable("weather")
    weatherDataFrame
  }


  private def loadRDD(sc: SparkContext,filePath: String): RDD[Array[String]] = {
    val rdd = sc.textFile(filePath)
    rdd.map(_.split("\t"))
  }

}
