import com.didi.config.CsvFilePath
import com.didi.service.DataFrameService
import org.apache.spark.sql.{DataFrame, SaveMode}

object App {


  def main(args: Array[String]) {

    val orderDataFrame = DataFrameService.loadOrderDataFrame()
    val trafficDataFrame = DataFrameService.loadTrafficDataFrame()
    val clusterDataFrame = DataFrameService.loadClusterDataFrame()
    val poiDataFrame = DataFrameService.loadPoiDataFrame()
    val weatherDataFrame = DataFrameService.loadWeatherDataFrame()


    val trafficWithRegion = clusterDataFrame.join(trafficDataFrame, clusterDataFrame("districtHash") === trafficDataFrame("districtHash"), "left")
    trafficWithRegion.show()


    val aij = orderDataFrame.groupBy("time").count()
    val rij = orderDataFrame.filter(orderDataFrame("driverId") !== "NULL").groupBy("time").count()

    println(aij.count())
    println(rij.count())

    //val fullOrderDataFrame = aij.join(rij, aij("time").as("t1") === rij("time").as("t2"), "left")
    //fullOrderDataFrame.show()

    //val df4 = fullOrderDataFrame.select($"aij.time".as("t1"), $"aij.count".as("t2"), "*")
    //df4.show()

    //val orderWithWeather = orderDataFrame.join(weatherDataFrame, orderDataFrame("time") === weatherDataFrame("time"), "left")
    //orderWithWeather.show()

    //trafficWithRegion.rdd.saveAsTextFile(CsvFilePath.saveMergeFilePath)


    //TODO dump data frame to csv, invoke the saveDataFrame2CSV method
  }



  private def saveDataFrame2CSV(fullOrderDataFrame: DataFrame): Unit  = {
    val saveOptions = Map("header" -> "true", "path" -> "train.csv")
    val fullOrder = fullOrderDataFrame.select(fullOrderDataFrame("time").as("time"), fullOrderDataFrame("count"))
    fullOrder.write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(saveOptions).save()
  }

}
