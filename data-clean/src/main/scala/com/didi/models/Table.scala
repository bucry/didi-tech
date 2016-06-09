package com.didi.models

/**
  * Created by twcn on 6/9/16.
  */

case class Table(orderId: String,
                 driverId: String,
                 passengerId: String,
                 startRegion: String,
                 destRegion: String,
                 price: Double,
                 time: String,
                 poiClass: String,
                 tjLevel: String,
                 weather: Int,
                 temperature: Int,
                 pm2point5: Int
                )
