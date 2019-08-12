package com.jcdecaux.datacorp.spark.workflow

import java.util.UUID

/**
  * External data source
  */
sealed abstract class External

object External extends Node(classOf[External], UUID.fromString("00000000-0000-0000-0000-000000000000"), -1, List(), null)
