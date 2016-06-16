package com.didi.train

import java.util.UUID

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DoubleType, StringType, StructField}

object Wrapper {

  final val EnumMetadataName: String = "isEnum"
  final val LabelMetadataName: String = "isLabel"
  final val EnumColumnTail: String = "_Enum"
  final val VectorColumnTail: String = "_Vector"

  def dataFrame2Stages(dataFrameWithMetadata: DataFrame): Array[PipelineStage] = {
    val columnStages = for {
      StructField(name, dataType, nullable, metadata) <- dataFrameWithMetadata.schema.fields
      if !metadata.contains(LabelMetadataName) || !metadata.getBoolean(LabelMetadataName)
      if metadata.contains(EnumMetadataName) && metadata.getBoolean(EnumMetadataName)
    } yield {
      dataType match {
        case StringType =>

          val stringIndexer = new StringIndexer().
            setInputCol(name).
            setOutputCol(UUID.randomUUID().toString)

          val oneHotEncoder = new OneHotEncoder().
            setDropLast(false).
            setInputCol(stringIndexer.getOutputCol).
            setOutputCol(UUID.randomUUID().toString)

          new Pipeline().setStages(Array(
            stringIndexer, oneHotEncoder
          ))
        case DoubleType =>
          new OneHotEncoder().
            setDropLast(false).
            setInputCol(name).
            setOutputCol(UUID.randomUUID().toString)
      }
    }
    columnStages
  }
}
