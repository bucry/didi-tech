package com.didi.models


case class Table()

case class FeatureTable()

case class Order(orderId: String, driverId: String, passengerId: String, startRegion: String, destRegion: String, price: Double, time: String)

case class Cluster(districtHash: String, districtId: String)

case class Poi(districtHash: String, poiClass: String)

case class Traffic(districtHash: String, tjLevel: String, tjTime: String)

case class Weather(time: String, weather: Int, temperature: Double, pm2p5: Double)

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
                          )

final case class OrderFeatureTable()
