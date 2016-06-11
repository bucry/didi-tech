package com.didi.train

import com.didi.models.{OrderFeatureTable, Table}


object FeatureExtraction {
  def extract(table: Table): OrderFeatureTable = {
    table match {
      case _ => OrderFeatureTable()
    }
  }
}
