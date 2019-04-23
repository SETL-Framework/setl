package com.jcdecaux.datacorp.spark

case class TestDwellTime(asset: String,
                         country: String,
                         location: String,
                         dwell_time: String,
                         date: java.sql.Date,
                         percent: Float)
