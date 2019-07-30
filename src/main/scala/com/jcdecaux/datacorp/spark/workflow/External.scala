package com.jcdecaux.datacorp.spark.workflow
/**
  * External data source
  */
abstract class External

object External extends Node(classOf[External], "", -1, List(), null)
