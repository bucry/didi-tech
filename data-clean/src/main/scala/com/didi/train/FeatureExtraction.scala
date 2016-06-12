package com.didi.train

import com.didi.models.{FeatureTable, OrderFeatureTable, Table}


object FeatureExtraction {
  def extract(table: Table): FeatureTable = {
    table match {
      case _ => OrderFeatureTable()
    }
  }
}
