package com.simiacryptus.mindseye.tensorflow

import com.simiacryptus.mindseye.lang.Layer

case class LayerID
(
  layerId: String,
  layerName: String,
  layerClass: String
) {
  def this(layer: Layer) = this(layer.getId.toString, layer.getName, layer.getClass.getCanonicalName)
}
