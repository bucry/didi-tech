package com.didi.merge

import com.didi.service.ConvertRDDService
import org.apache.spark.rdd.RDD

/**
  * Created by twcn on 6/8/16.
  */
object Merge {


  def mergeRDD(orderRDD: RDD[Array[String]],
               trafficRDD: RDD[Array[String]],
               clusterRDD: RDD[Array[String]],
               poiRDD: RDD[Array[String]],
               weatherRDD: RDD[Array[String]]): RDD[Array[String]] = {
    ???
  }


  private def getOrderKeyValueRDD(orderRDD: RDD[Array[String]]): RDD[(String, Array[String])] = {
    ConvertRDDService.covertRDD2KeyValueRDD(orderRDD, 1)
  }

}

