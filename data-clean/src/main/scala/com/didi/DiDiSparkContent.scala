package com.didi

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object DiDiSparkContent {
  val conf = new SparkConf().setMaster("local[8]").setAppName("DIDI-TECH")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
}
