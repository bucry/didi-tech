import com.didi.config.CsvFilePath
import com.didi.service.DataFrameService
import org.apache.spark.sql.SaveMode

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
    val rij = orderDataFrame.filter(orderDataFrame("driverId").isNotNull).groupBy("time").count()
    val fullOrderDataFrame = aij.join(rij, aij("time") === rij("time"), "left")
    fullOrderDataFrame.show()

    val saveOptions = Map("header" -> "true", "path" -> "items.csv")
    val fullOrder = fullOrderDataFrame.select(fullOrderDataFrame("time").as("time"), fullOrderDataFrame("count"))
    fullOrder.write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).options(saveOptions).save()

    //val orderWithWeather = orderDataFrame.join(weatherDataFrame, orderDataFrame("time") === weatherDataFrame("time"), "left")
    //orderWithWeather.show()

    //trafficWithRegion.rdd.saveAsTextFile(CsvFilePath.saveMergeFilePath)
  }

}
