package com.didi.merge

import org.apache.spark.sql.DataFrame

object Flatter {

  abstract class Join(column: String)

  final case class Left(column: String) extends Join(column)
  final case class Right(column: String) extends Join(column)
  final case class Inner(column: String) extends Join(column)
  final case class Out(column: String) extends Join(column)


  def join(letDataFrame: DataFrame, rightDataFrame: DataFrame, where: Join): DataFrame = {
    where match {
      case Left(column) => joinLeft(letDataFrame, rightDataFrame, column)
      case Right(column) => joinRight(letDataFrame, rightDataFrame, column)
      case _ => letDataFrame
    }


  }

  private def joinLeft(letDataFrame: DataFrame, rightDataFrame: DataFrame, column: String): DataFrame = {
    letDataFrame.join(rightDataFrame, letDataFrame(column) === rightDataFrame(column), "left")
  }

  private def joinRight(letDataFrame: DataFrame, rightDataFrame: DataFrame, column: String): DataFrame = {
    letDataFrame.join(rightDataFrame, letDataFrame(column) === rightDataFrame(column), "right")
  }

}
