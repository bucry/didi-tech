package com.didi.models


abstract class Table()

abstract class FeatureTable()

case class Order(orderId: String, driverId: String, passengerId: String, startRegion: String, destRegion: String, price: Double, time: String) extends Table()

case class Cluster(districtHash: String, districtId: String)  extends Table()

case class Poi(districtHash: String, poiClass: String)  extends Table()

case class Traffic(districtHash: String, tjLevel: String, tjTime: String)  extends Table()

case class Weather(time: String, weather: Int, temperature: Double, pm2p5: Double)  extends Table()

final case class FullOrder(
                           orderId: String,
                           driverId: String,
                           passengerId: String,
                           startRegion: String,
                           startRegionId: String,
                           startRegionTjLevel: String,
                           destRegion: String,
                           destRegionId: String,
                           destRegionTjLevel: String,
                           price: Double,
                           weather: Int,
                           temperature: Double,
                           pm2p5: Double,
                           poiClass: String,
                           time: String
                          )  extends Table()

final case class OrderFeatureTable() extends FeatureTable()
