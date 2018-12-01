package com.simiacryptus.mindseye.dataframes

import com.simiacryptus.mindseye.lang.{Layer, Tensor}

import scala.collection.mutable

final class ModelingData extends Serializable {
  val representations: mutable.HashMap[String, Tensor] = new mutable.HashMap[String, Tensor]()
  val transforms: mutable.HashMap[String, Layer] = new mutable.HashMap[String, Layer]()
}
