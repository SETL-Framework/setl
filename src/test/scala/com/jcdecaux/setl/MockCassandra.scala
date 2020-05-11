package com.jcdecaux.setl

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.SparkConf

class MockCassandra(connector: CassandraConnector, keyspace: String) {

  private def dropTable(table: String, session: CqlSession): Unit = session.execute(s"DROP TABLE IF EXISTS $keyspace.$table;")

  def dropKeyspace(): this.type = {
    connector.withSessionDo(session => {
      session.execute(s"DROP KEYSPACE IF EXISTS $keyspace;")
    })
    this
  }

  def generateKeyspace(): this.type = {
    connector.withSessionDo(session => {
      session.execute(s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};")
    })

    this
  }

  def generateAsset(table: String): this.type = {
    println("generating asset table")
    connector.withSessionDo { session =>

      this.dropTable(table, session)

      session.execute(
        s"""
           |CREATE TABLE IF NOT EXISTS $keyspace.$table (
           |    country text,
           |    asset text,
           |    density text,
           |    environment text,
           |    is_digital boolean,
           |    is_loop boolean,
           |    latitude double,
           |    location text,
           |    longitude double,
           |    nb_slots int,
           |    slot_duration int,
           |    surface float,
           |    PRIMARY KEY ((country, asset))
           |);
         """.stripMargin)

      session.execute(s"INSERT INTO $keyspace.$table(country, asset, density, environment, is_digital, is_loop, latitude, location, longitude, nb_slots, slot_duration, surface) VALUES ('SG', 'asset-1', 'low', 'airport', true, true, 1.1, 'SIN-T1', 1.2, 8, 10, 2);")
      session.execute(s"INSERT INTO $keyspace.$table(country, asset, density, environment, is_digital, is_loop, latitude, location, longitude, nb_slots, slot_duration, surface) VALUES ('SG', 'asset-2', 'low', 'airport', true, true, 1.1, 'SIN-T1', 1.2, 9, 10, 2);")
      session.execute(s"INSERT INTO $keyspace.$table(country, asset, density, environment, is_digital, is_loop, latitude, location, longitude, nb_slots, slot_duration, surface) VALUES ('SG', 'asset-3', 'low', 'airport', true, true, 1.1, 'SIN-T1', 1.2, 9, 10, 2);")
      session.execute(s"INSERT INTO $keyspace.$table(country, asset, density, environment, is_digital, is_loop, latitude, location, longitude, nb_slots, slot_duration, surface) VALUES ('SG', 'asset-4', 'low', 'airport', true, true, 1.1, 'SIN-T1', 1.2, 9, 15, 2);")
      session.execute(s"INSERT INTO $keyspace.$table(country, asset, density, environment, is_digital, is_loop, latitude, location, longitude, nb_slots, slot_duration, surface) VALUES ('SG', 'asset-5', 'low', 'airport', true, true, 1.1, 'SIN-T2', 1.2, 8, 10, 3);")
      session.execute(s"INSERT INTO $keyspace.$table(country, asset, density, environment, is_digital, is_loop, latitude, location, longitude, nb_slots, slot_duration, surface) VALUES ('SG', 'asset-6', 'low', 'airport', true, true, 1.1, 'SIN-T2', 1.2, 9, 10, 3);")
      session.execute(s"INSERT INTO $keyspace.$table(country, asset, density, environment, is_digital, is_loop, latitude, location, longitude, nb_slots, slot_duration, surface) VALUES ('SG', 'asset-7', 'low', 'airport', true, true, 1.1, 'SIN-T2', 1.2, 10, 10, 3);")
      session.execute(s"INSERT INTO $keyspace.$table(country, asset, density, environment, is_digital, is_loop, latitude, location, longitude, nb_slots, slot_duration, surface) VALUES ('HK', 'asset-8', 'low', 'mall', true, true, 1.1, 'HKG-T1', 1.2, 8, 10, 4);")
      session.execute(s"INSERT INTO $keyspace.$table(country, asset, density, environment, is_digital, is_loop, latitude, location, longitude, nb_slots, slot_duration, surface) VALUES ('HK', 'asset-9', 'low', 'mall', true, true, 1.1, 'HKG-T2', 1.2, 9, 10, 4);")
    }

    this
  }


  def generateDwellTime(table: String): this.type = {
    connector.withSessionDo(session => {
      this.dropTable(table, session)
      session.execute(
        s"""
           |CREATE TABLE IF NOT EXISTS $keyspace.$table (
           |    country text,
           |    location text,
           |    asset text,
           |    dwell_time text,
           |    date date,
           |    percent float,
           |    PRIMARY KEY ((country, location, asset, dwell_time), date)
           |);
         """.stripMargin
      )

      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, dwell_time, date, percent) VALUES ('SG', 'SIN-T1', 'asset-1', '0-5',     '2018-01-01', 0.4);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, dwell_time, date, percent) VALUES ('SG', 'SIN-T1', 'asset-1', '5-10',    '2018-01-01', 0.2);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, dwell_time, date, percent) VALUES ('SG', 'SIN-T1', 'asset-1', '10-15',   '2018-01-01', 0.1);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, dwell_time, date, percent) VALUES ('SG', 'SIN-T1', 'asset-1', '15-30',   '2018-01-01', 0.1);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, dwell_time, date, percent) VALUES ('SG', 'SIN-T1', 'asset-1', '30-60',   '2018-01-01', 0.1);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, dwell_time, date, percent) VALUES ('SG', 'SIN-T1', 'asset-1', '60-120',  '2018-01-01', 0.05);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, dwell_time, date, percent) VALUES ('SG', 'SIN-T1', 'asset-1', '120',     '2018-01-01', 0.05);")

      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, dwell_time, date, percent) VALUES ('SG', 'SIN-T1', 'asset-2', '0-5',     '2018-01-01', 0.4);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, dwell_time, date, percent) VALUES ('SG', 'SIN-T1', 'asset-2', '5-10',    '2018-01-01', 0.2);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, dwell_time, date, percent) VALUES ('SG', 'SIN-T1', 'asset-2', '10-15',   '2018-01-01', 0.1);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, dwell_time, date, percent) VALUES ('SG', 'SIN-T1', 'asset-2', '15-30',   '2018-01-01', 0.1);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, dwell_time, date, percent) VALUES ('SG', 'SIN-T1', 'asset-2', '30-60',   '2018-01-01', 0.1);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, dwell_time, date, percent) VALUES ('SG', 'SIN-T1', 'asset-2', '60-120',  '2018-01-01', 0.05);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, dwell_time, date, percent) VALUES ('SG', 'SIN-T1', 'asset-2', '120',     '2018-01-01', 0.05);")

      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, dwell_time, date, percent) VALUES ('SG', 'SIN-T1', 'asset-3', '0-5',     '2018-01-01', 0.1);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, dwell_time, date, percent) VALUES ('SG', 'SIN-T1', 'asset-3', '5-10',    '2018-01-01', 0.2);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, dwell_time, date, percent) VALUES ('SG', 'SIN-T1', 'asset-3', '10-15',   '2018-01-01', 0.1);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, dwell_time, date, percent) VALUES ('SG', 'SIN-T1', 'asset-3', '15-30',   '2018-01-01', 0.1);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, dwell_time, date, percent) VALUES ('SG', 'SIN-T1', 'asset-3', '30-60',   '2018-01-01', 0.4);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, dwell_time, date, percent) VALUES ('SG', 'SIN-T1', 'asset-3', '60-120',  '2018-01-01', 0.05);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, dwell_time, date, percent) VALUES ('SG', 'SIN-T1', 'asset-3', '120',     '2018-01-01', 0.05);")

      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, dwell_time, date, percent) VALUES ('SG', 'SIN-T1', 'asset-4', '0-5',     '2018-01-01', 0.4);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, dwell_time, date, percent) VALUES ('SG', 'SIN-T1', 'asset-4', '5-10',    '2018-01-01', 0.2);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, dwell_time, date, percent) VALUES ('SG', 'SIN-T1', 'asset-4', '10-15',   '2018-01-01', 0.1);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, dwell_time, date, percent) VALUES ('SG', 'SIN-T1', 'asset-4', '15-30',   '2018-01-01', 0.1);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, dwell_time, date, percent) VALUES ('SG', 'SIN-T1', 'asset-4', '30-60',   '2018-01-01', 0.1);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, dwell_time, date, percent) VALUES ('SG', 'SIN-T1', 'asset-4', '60-120',  '2018-01-01', 0.05);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, dwell_time, date, percent) VALUES ('SG', 'SIN-T1', 'asset-4', '120',     '2018-01-01', 0.05);")

      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, dwell_time, date, percent) VALUES ('SG', 'SIN-T1', 'asset-4', '0-5',     '2018-01-02', 0.4);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, dwell_time, date, percent) VALUES ('SG', 'SIN-T1', 'asset-4', '5-10',    '2018-01-02', 0.2);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, dwell_time, date, percent) VALUES ('SG', 'SIN-T1', 'asset-4', '10-15',   '2018-01-02', 0.1);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, dwell_time, date, percent) VALUES ('SG', 'SIN-T1', 'asset-4', '15-30',   '2018-01-02', 0.1);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, dwell_time, date, percent) VALUES ('SG', 'SIN-T1', 'asset-4', '30-60',   '2018-01-02', 0.1);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, dwell_time, date, percent) VALUES ('SG', 'SIN-T1', 'asset-4', '60-120',  '2018-01-02', 0.05);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, dwell_time, date, percent) VALUES ('SG', 'SIN-T1', 'asset-4', '120',     '2018-01-02', 0.05);")

      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, dwell_time, date, percent) VALUES ('SG', 'SIN-T1', 'asset-4', '0-5',     '2018-01-03', 0.4);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, dwell_time, date, percent) VALUES ('SG', 'SIN-T1', 'asset-4', '5-10',    '2018-01-03', 0.2);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, dwell_time, date, percent) VALUES ('SG', 'SIN-T1', 'asset-4', '10-15',   '2018-01-03', 0.1);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, dwell_time, date, percent) VALUES ('SG', 'SIN-T1', 'asset-4', '15-30',   '2018-01-03', 0.1);")


    })

    this
  }

  def generateTraffic(table: String): this.type = {
    connector.withSessionDo(session => {
      this.dropTable(table, session)
      session.execute(
        s"""
           |CREATE TABLE IF NOT EXISTS $keyspace.$table (
           |    country text,
           |    location text,
           |    asset text,
           |    datetime timestamp,
           |    pedestrian_traffic bigint,
           |    traffic bigint,
           |    PRIMARY KEY ((country, location, asset), datetime)
           |);
         """.stripMargin
      )

      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-1', '2018-01-01 00:00:00+0000', 100, 200);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-1', '2018-01-01 01:00:00+0000', 100, 200);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-1', '2018-01-01 02:00:00+0000', 100, 200);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-1', '2018-01-01 03:00:00+0000', 100, 200);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-1', '2018-01-01 04:00:00+0000', 100, 200);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-1', '2018-01-01 05:00:00+0000', 100, 200);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-1', '2018-01-01 06:00:00+0000', 100, 200);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-1', '2018-01-01 07:00:00+0000', 100, 200);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-1', '2018-01-01 08:00:00+0000', 100, 200);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-1', '2018-01-01 09:00:00+0000', 100, 200);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-1', '2018-01-01 10:00:00+0000', 100, 200);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-1', '2018-01-01 11:00:00+0000', 100, 200);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-1', '2018-01-01 12:00:00+0000', 300, 400);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-1', '2018-01-01 13:00:00+0000', 300, 400);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-1', '2018-01-01 14:00:00+0000', 300, 400);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-1', '2018-01-01 15:00:00+0000', 300, 400);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-1', '2018-01-01 16:00:00+0000', 300, 400);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-1', '2018-01-01 17:00:00+0000', 300, 400);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-1', '2018-01-01 18:00:00+0000', 300, 400);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-1', '2018-01-01 19:00:00+0000', 300, 400);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-1', '2018-01-01 20:00:00+0000', 300, 400);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-1', '2018-01-01 21:00:00+0000', 300, 400);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-1', '2018-01-01 22:00:00+0000', 300, 400);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-1', '2018-01-01 23:00:00+0000', 300, 400);")

      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-2', '2018-01-01 00:00:00+0000', 100, 200);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-2', '2018-01-01 01:00:00+0000', 100, 200);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-2', '2018-01-01 02:00:00+0000', 100, 200);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-2', '2018-01-01 03:00:00+0000', 100, 200);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-2', '2018-01-01 04:00:00+0000', 100, 200);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-2', '2018-01-01 05:00:00+0000', 100, 200);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-2', '2018-01-01 06:00:00+0000', 100, 200);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-2', '2018-01-01 07:00:00+0000', 100, 200);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-2', '2018-01-01 08:00:00+0000', 100, 200);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-2', '2018-01-01 09:00:00+0000', 100, 200);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-2', '2018-01-01 10:00:00+0000', 100, 200);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-2', '2018-01-01 11:00:00+0000', 100, 200);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-2', '2018-01-01 12:00:00+0000', 300, 400);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-2', '2018-01-01 13:00:00+0000', 300, 400);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-2', '2018-01-01 14:00:00+0000', 300, 400);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-2', '2018-01-01 15:00:00+0000', 300, 400);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-2', '2018-01-01 16:00:00+0000', 300, 400);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-2', '2018-01-01 17:00:00+0000', 300, 400);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-2', '2018-01-01 18:00:00+0000', 300, 400);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-2', '2018-01-01 19:00:00+0000', 300, 400);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-2', '2018-01-01 20:00:00+0000', 300, 400);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-2', '2018-01-01 21:00:00+0000', 300, 400);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-2', '2018-01-01 22:00:00+0000', 300, 400);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-2', '2018-01-01 23:00:00+0000', 300, 400);")

      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-3', '2018-01-01 00:00:00+0000', 100, 200);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-3', '2018-01-01 01:00:00+0000', 100, 200);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-3', '2018-01-01 02:00:00+0000', 100, 200);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-3', '2018-01-01 03:00:00+0000', 100, 200);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-3', '2018-01-01 04:00:00+0000', 100, 200);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-3', '2018-01-01 05:00:00+0000', 100, 200);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-3', '2018-01-01 06:00:00+0000', 100, 200);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-3', '2018-01-01 07:00:00+0000', 100, 200);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-3', '2018-01-01 08:00:00+0000', 100, 200);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-3', '2018-01-01 09:00:00+0000', 100, 200);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-3', '2018-01-01 10:00:00+0000', 100, 200);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-3', '2018-01-01 11:00:00+0000', 100, 200);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-3', '2018-01-01 12:00:00+0000', 300, 400);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-3', '2018-01-01 13:00:00+0000', 300, 400);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-3', '2018-01-01 14:00:00+0000', 300, 400);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-3', '2018-01-01 15:00:00+0000', 300, 400);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-3', '2018-01-01 16:00:00+0000', 300, 400);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-3', '2018-01-01 17:00:00+0000', 300, 400);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-3', '2018-01-01 18:00:00+0000', 300, 400);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-3', '2018-01-01 19:00:00+0000', 300, 400);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-3', '2018-01-01 20:00:00+0000', 300, 400);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-3', '2018-01-01 21:00:00+0000', 300, 400);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-3', '2018-01-01 22:00:00+0000', 300, 400);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-3', '2018-01-01 23:00:00+0000', 300, 400);")

      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-4', '2018-01-01 00:00:00+0000', 100, 200);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-4', '2018-01-01 01:00:00+0000', 100, 200);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-4', '2018-01-01 02:00:00+0000', 100, 200);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-4', '2018-01-01 03:00:00+0000', 100, 200);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-4', '2018-01-01 04:00:00+0000', 100, 200);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-4', '2018-01-01 05:00:00+0000', 100, 200);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-4', '2018-01-01 06:00:00+0000', 100, 200);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-4', '2018-01-01 07:00:00+0000', 100, 200);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-4', '2018-01-01 08:00:00+0000', 100, 200);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-4', '2018-01-01 09:00:00+0000', 100, 200);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-4', '2018-01-01 10:00:00+0000', 100, 200);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-4', '2018-01-01 11:00:00+0000', 100, 200);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-4', '2018-01-01 12:00:00+0000', 300, 400);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-4', '2018-01-01 13:00:00+0000', 300, 400);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-4', '2018-01-01 14:00:00+0000', 300, 400);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-4', '2018-01-01 15:00:00+0000', 300, 400);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-4', '2018-01-01 16:00:00+0000', 300, 400);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-4', '2018-01-01 17:00:00+0000', 300, 400);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-4', '2018-01-01 18:00:00+0000', 300, 400);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-4', '2018-01-01 19:00:00+0000', 300, 400);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-4', '2018-01-01 20:00:00+0000', 300, 400);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-4', '2018-01-01 21:00:00+0000', 300, 400);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-4', '2018-01-01 22:00:00+0000', 300, 400);")
      session.execute(s"INSERT INTO $keyspace.$table(country, location, asset, datetime, pedestrian_traffic, traffic) VALUES ('SG', 'SIN-T1', 'asset-4', '2018-01-01 23:00:00+0000', 300, 400);")
    })

    this
  }

  def generateExpectedImpression(table: String): this.type = {
    connector.withSessionDo(session => {
      this.dropTable(table, session)
      session.execute(
        s"""
           |CREATE TABLE IF NOT EXISTS $keyspace.$table (
           |    country text,
           |    date date,
           |    share_of_time float,
           |    location text,
           |    asset text,
           |    exp_otc_dwell double,
           |    exp_otc_flow double,
           |    exp_vac_dwell double,
           |    exp_vac_flow double,
           |    PRIMARY KEY (country, date, share_of_time, location, asset)
           |);
         """.stripMargin
      )

      session.execute(s"INSERT INTO $keyspace.$table(country, date, share_of_time, location, asset, exp_otc_dwell, exp_otc_flow, exp_vac_dwell, exp_vac_flow) VALUES ('SG', '2018-01-01', 0.1, 'SIN-T1', 'asset-1', 1.5, 1.1, 1.3, 1.05);")
      session.execute(s"INSERT INTO $keyspace.$table(country, date, share_of_time, location, asset, exp_otc_dwell, exp_otc_flow, exp_vac_dwell, exp_vac_flow) VALUES ('SG', '2018-01-01', 0.1, 'SIN-T1', 'asset-2', 1.5, 1.1, 1.3, 1.05);")
      session.execute(s"INSERT INTO $keyspace.$table(country, date, share_of_time, location, asset, exp_otc_dwell, exp_otc_flow, exp_vac_dwell, exp_vac_flow) VALUES ('SG', '2018-01-01', 0.1, 'SIN-T1', 'asset-3', 1.5, 1.1, 1.3, 1.05);")
      session.execute(s"INSERT INTO $keyspace.$table(country, date, share_of_time, location, asset, exp_otc_dwell, exp_otc_flow, exp_vac_dwell, exp_vac_flow) VALUES ('SG', '2018-01-01', 0.1, 'SIN-T1', 'asset-4', 1.5, 1.1, 1.3, 1.05);")
    })
    this
  }

  def generateCountry(table: String): this.type = {
    connector.withSessionDo(session => {
      this.dropTable(table, session)
      session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$table(iata varchar, country varchar, PRIMARY KEY(iata));")

      session.execute(s"INSERT INTO $keyspace.$table(iata, country) VALUES ('UGB', 'US');")
      session.execute(s"INSERT INTO $keyspace.$table(iata, country) VALUES ('EUG', 'US');")
      session.execute(s"INSERT INTO $keyspace.$table(iata, country) VALUES ('AGO', 'US');")
      session.execute(s"INSERT INTO $keyspace.$table(iata, country) VALUES ('CTS', 'JP');")
      session.execute(s"INSERT INTO $keyspace.$table(iata, country) VALUES ('QVR', 'BR');")
      session.execute(s"INSERT INTO $keyspace.$table(iata, country) VALUES ('TTE', 'ID');")
      session.execute(s"INSERT INTO $keyspace.$table(iata, country) VALUES ('JBT', 'US');")
      session.execute(s"INSERT INTO $keyspace.$table(iata, country) VALUES ('SVE', 'US');")
      session.execute(s"INSERT INTO $keyspace.$table(iata, country) VALUES ('SVF', 'BJ');")
      session.execute(s"INSERT INTO $keyspace.$table(iata, country) VALUES ('MJU', 'ID');")
      session.execute(s"INSERT INTO $keyspace.$table(iata, country) VALUES ('KHD', 'IR');")
      session.execute(s"INSERT INTO $keyspace.$table(iata, country) VALUES ('GSM', 'IR');")
      session.execute(s"INSERT INTO $keyspace.$table(iata, country) VALUES ('LUU', 'AU');")
      session.execute(s"INSERT INTO $keyspace.$table(iata, country) VALUES ('YQQ', 'CA');")
      session.execute(s"INSERT INTO $keyspace.$table(iata, country) VALUES ('ERN', 'BR');")
      session.execute(s"INSERT INTO $keyspace.$table(iata, country) VALUES ('TSM', 'US');")
      session.execute(s"INSERT INTO $keyspace.$table(iata, country) VALUES ('NTD', 'US');")
      session.execute(s"INSERT INTO $keyspace.$table(iata, country) VALUES ('MRC', 'US');")
      session.execute(s"INSERT INTO $keyspace.$table(iata, country) VALUES ('NKD', 'ID');")
      session.execute(s"INSERT INTO $keyspace.$table(iata, country) VALUES ('LGE', 'AU');")
    })
    this
  }

  def generateEmptyTestTable(table: String): this.type = {
    connector.withSessionDo(session => {
      this.dropTable(table, session)
      session.execute(s"CREATE TABLE $keyspace.$table(asset text, surface float, nb_slots int, PRIMARY KEY(asset));")
    })
    this
  }

}

object MockCassandra {
  val keyspace: String = "test_space"
  val host: String = "localhost" // System.getProperty("setl.test.cassandra.host", "localhost")

  val cassandraConf: SparkConf = new SparkConf(true)
    .set("spark.cassandra.connection.host", host)
    .set("spark.cassandra.connection.port", "9042")
    .set("spark.ui.showConsoleProgress", "false")
    .set("spark.ui.enabled", "false")
    .set("spark.cleaner.ttl", "3600")
    .setMaster("local[*]")
    .setAppName("Test")
}
