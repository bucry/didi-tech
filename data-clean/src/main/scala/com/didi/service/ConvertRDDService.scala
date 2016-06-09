package com.didi.service

import org.apache.spark.rdd.RDD

/**
  * Created by twcn on 6/8/16.
  */
object ConvertRDDService {

  def covertRDD2KeyValueRDD(originRDD: RDD[Array[String]], index: Int): RDD[(String, Array[String])] = {
    originRDD.map {
      row => row(index) -> row
    }
  }

}
