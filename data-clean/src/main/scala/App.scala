import com.didi.service.DataFrameService

object App {


  def main(args: Array[String]) {
    val orderDataFrame = DataFrameService.loadOrderDataFrame()
    val trafficDataFrame = DataFrameService.loadTrafficDataFrame()
    val clusterDataFrame = DataFrameService.loadClusterDataFrame()
    val poiDataFrame = DataFrameService.loadPoiDataFrame()
    val weatherDataFrame = DataFrameService.loadWeatherDataFrame()


    val trafficWithRegion = clusterDataFrame.join(trafficDataFrame, clusterDataFrame("districtHash") === trafficDataFrame("districtHash"), "left")
    trafficWithRegion.show()
    val s = orderDataFrame.join(weatherDataFrame, orderDataFrame("time") === weatherDataFrame("time"), "left")
    s.show()
  }

}
