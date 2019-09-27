package com.jcdecaux.datacorp.spark.workflow

import java.util.UUID

import com.jcdecaux.datacorp.spark.transformation.Factory

/**
  * External data source
  */
sealed abstract class External extends Factory[External]

object External extends Node(classOf[External], UUID.fromString("00000000-0000-0000-0000-000000000000"), -1, List(), null)
