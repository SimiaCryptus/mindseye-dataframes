/*
 * Copyright (c) 2019 by Andrew Charneski.
 *
 * The author licenses this file to you under the
 * Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy
 * of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.simiacryptus.mindseye.tensorflow

import java.util.UUID

import com.simiacryptus.mindseye.lang.{Layer, Tensor}

class CategorizingModelingStrategy(categoryColumnName: String, categories: Int, val defaultSize: Int*) extends RDDModelingStrategy(defaultSize: _*) {
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
