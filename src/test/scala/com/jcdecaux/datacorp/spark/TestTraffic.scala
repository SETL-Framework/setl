package com.jcdecaux.datacorp.spark

case class TestTraffic(asset: String,
                       country: String,
                       location: String,
                       datetime: java.sql.Timestamp,
                       pedestrian_traffic: Double,
                       traffic: Double)
