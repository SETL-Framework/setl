package com.jcdecaux.datacorp.spark.workflow

import com.jcdecaux.datacorp.spark.transformation.Factory

import scala.collection.mutable.ArrayBuffer

class Pipeline2 {

  var stages: ArrayBuffer[Stage] = ArrayBuffer[Stage]()

  def addStage(factory: Factory[_]): this.type = {
    addStage(new Stage().addFactory(factory))
    this
  }

  def addStage(stage: Stage): this.type = {
    stages += stage
    this
  }


}