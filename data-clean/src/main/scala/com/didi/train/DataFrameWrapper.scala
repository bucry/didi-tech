package com.didi.train

import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{Column, DataFrame}

object DataFrameWrapper {

  def int2Double(dataFrame: DataFrame): DataFrame = {
    import dataFrame.sqlContext.implicits._
    import org.apache.spark.sql.functions._
    val toDouble = udf[Double, Int]( _.toDouble)
    val (newDataFrame, newColumnNames) = dataFrame.schema.foldLeft[(DataFrame, List[Column])]((dataFrame, List())) { case ((last, listColumns), column) => {
      if (column.dataType == IntegerType) {
        val newCloumn = column.name + "_new"
        (last.withColumn(newCloumn, toDouble(last(column.name))), ($"${newCloumn}" as column.name) ::listColumns)
      } else {
        (last, ($"${column.name}" as column.name) :: listColumns)
      }
    }
    }
    newDataFrame.select(newColumnNames : _*)
  }
}
