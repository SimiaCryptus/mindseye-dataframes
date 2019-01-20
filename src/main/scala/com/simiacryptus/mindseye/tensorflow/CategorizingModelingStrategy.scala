package com.simiacryptus.mindseye.tensorflow

import java.util.UUID

import com.simiacryptus.mindseye.lang.{Layer, Tensor}

class CategorizingModelingStrategy(categoryColumnName: String, categories:Int, val defaultSize: Int*) extends RDDModelingStrategy(defaultSize: _*) {
  override def initialRepresentation(value: String): Tensor = {
    if (value.startsWith(categoryColumnName)) {
      require(null != value)
      val id = UUID.nameUUIDFromBytes(value.getBytes("UTF-8"))
      val initData = new Tensor(categories)
      initData.setAll(0.0)
      initData.set(value.split("=").reverse.head.toInt - 1, 1.0)
      initData.setId(id)
      require(null != initData)
      logger.debug(s"Initialize category for $value of $categories ($id) = $initData")
      initData
    } else {
      super.initialRepresentation(value)
    }
  }

  override def edit(ctx: DataframeModeler, layer: Layer): Layer = {
    if (ctx.path.startsWith(categoryColumnName)) {
      //logger.info(s"Setting ${layer} frozen at ${ctx.path}")
      layer.setFrozen(true)
    } else {
      layer
    }
  }
}
