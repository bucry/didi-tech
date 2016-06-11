package com.didi.service

import org.scalatest.FunSuite


class DataFrameService$Test extends FunSuite {

  test("testLoadWeatherDataFrame") {
    val weatherDataFrame = DataFrameService.loadWeatherDataFrame()
    assert(weatherDataFrame.count() == 4811)
  }

  test("testLoadTrafficDataFrame") {
    val trafficDataFrame = DataFrameService.loadTrafficDataFrame()
    assert(trafficDataFrame.count() == 193553)
  }

  test("testLoadOrderDataFrame") {
    val orderDataFrame = DataFrameService.loadOrderDataFrame()
    assert(orderDataFrame.count() == 8540614)
  }

  test("testLoadPoiDataFrame") {
    val poiDataFrame = DataFrameService.loadPoiDataFrame()
    assert(poiDataFrame.count() == 66)

  }

  test("testLoadClusterDataFrame") {
    val clusterDataFrame = DataFrameService.loadClusterDataFrame()
    assert(clusterDataFrame.count() == 66)
  }

}
