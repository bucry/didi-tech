package com.didi.service

import java.text.SimpleDateFormat

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
    val orderDataFrame = orderRDD.map(f => Order(f(0), f(1), f(2), f(3), f(4), f(5).trim.toDouble, formatTimeSlice(f(6)))).toDF()
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

  private def formatTimeSlice(time: String): String = {
    val simpleDateFormatPrefix = new SimpleDateFormat("yyyy-MM-dd")
    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val diffTimestamp = (simpleDateFormat.parse(time).getTime() - simpleDateFormatPrefix.parse(time).getTime) / 1000 / 60
    val timeSlice = if (diffTimestamp / 10 == 0)  diffTimestamp / 10 else diffTimestamp / 10 + 1
    simpleDateFormatPrefix.format(simpleDateFormatPrefix.parse(time)) + "-" + timeSlice.toString
  }

}
