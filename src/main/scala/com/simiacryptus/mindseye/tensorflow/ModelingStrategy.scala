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

import java.util.function.BiConsumer
import java.util.{Random, UUID}

import com.google.gson.{GsonBuilder, JsonObject}
import com.simiacryptus.mindseye.lang._
import com.simiacryptus.mindseye.layers.java._
import com.simiacryptus.mindseye.network.PipelineNetwork
import com.simiacryptus.mindseye.tensorflow.DataframeModeler.{evalFeedback, zip}
import com.simiacryptus.sparkbook.util.Java8Util._
import com.simiacryptus.sparkbook.util.Logging
import com.simiacryptus.util.ArrayUtil
import org.apache.spark.sql.types.{ArrayType, DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

abstract class ModelingStrategy
(
  defaultSize: Int*
) extends Logging with Serializable {
  val mappingPower: Int = 2
  val structReduceLayer: Class[Layer] = Class.forName(classOf[SumInputsLayer].getCanonicalName).asInstanceOf[Class[Layer]] // or TensorConcatLayer

  def edit(ctx: DataframeModeler, layer: Layer): Layer = layer

  def initialRepresentation(value: String): Tensor = {
    require(null != value)
    val id = UUID.nameUUIDFromBytes(value.getBytes("UTF-8"))
    val hashCode = hash(value)
    val seeded = new Random(hashCode.asLong())
    val initData = new Tensor(size(value): _*)
    initData.set(() => seeded.nextDouble())
    initData.scaleInPlace(1.0 / initData.rms())
    initData.setId(id)
    require(null != initData)
    logger.debug(s"Initialize value for $value($id) = $initData")
    initData
  }

  def hash(value: String) = {
    DataframeModeler.rawHash(value)
  }

  def changeId[T <: Layer](in: T, id: UUID): T = {
    val newLayer = Layer.fromJson(new GsonBuilder().create().fromJson(in.getJson.toString.replaceAll(in.getId.toString, id.toString), classOf[JsonObject])).asInstanceOf[T]
    in.freeRef()
    newLayer
  }

  def initialTransform(path: String, stats: => (Double, Double, Double)) = {
    val id = UUID.nameUUIDFromBytes(path.getBytes("UTF-8"))
    val hashCode = hash(path)
    val seeded = new Random(hashCode.asLong())
    val pipelineNetwork = new PipelineNetwork(1)
    val head = pipelineNetwork.add(new LinearActivationLayer().setBias(-stats._2 / stats._3).setScale(1.0 / stats._3).freeze())
    pipelineNetwork.add(new TensorConcatLayer,
      (0 until mappingPower).map(i => pipelineNetwork.add(new NthPowerActivationLayer().setPower(i).setFrozen(true), head)): _*)
    pipelineNetwork.add(changeId(new FullyConnectedLayer(Array(mappingPower), Array(size(path): _*)), id))
    logger.debug(s"Initialize value for ${path}($id) = $pipelineNetwork")
    pipelineNetwork
  }

  def size(path: String): Seq[Int] = defaultSize

  def reduceStruct(ctx: DataframeModeler, results: Array[Result]): Result = {
    val layer = structReduceLayer.newInstance()
    val result = layer.evalAndFree(results: _*)
    layer.freeRef()
    result
  }

  def evalToDataframe(ctx: DataframeModeler, dataFrames: DataFrame*)(name: String, layers: Layer*)(implicit sparkSession: SparkSession): DataFrame

  def eval(ctx: DataframeModeler, dataFrames: DataFrame*)(layers: Layer*)(implicit sparkSession: SparkSession): Result
}

class LocalModelingStrategy(defaultSize: Int*) extends ModelingStrategy(defaultSize:_*) {
  override def evalToDataframe(ctx: DataframeModeler, dataFrames: DataFrame*)(name: String, layers: Layer*)(implicit sparkSession: SparkSession): DataFrame = {
    val (representationKeys, transformKeys) = ctx.initKeys(dataFrames: _*)
    val schema = dataFrames.map(_.schema).toArray
    val results = zip(dataFrames.map(_.rdd): _*).collect().grouped(1000).flatMap(itr => {
      val list = itr.toList
      val inputs: Seq[Result] = (0 until list.head.size).map(i => {
        val rows = list.map(_ (i))
        ctx.convertToResults(schema(i), rows)
      })
      val result = layers.foldLeft(inputs)((a: Seq[Result], b: Layer) => List(b.evalAndFree(a: _*))).head.getDataAndFree
      (0 until result.length()).map(result.get(_))
    })
    sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(results.map((t: Tensor) => Row((0 until t.length()).map(i => t.get(i)).toList)).toSeq),
      StructType(Array(
        StructField(name, ArrayType(DoubleType))
      ))
    )
  }

  override def eval(ctx: DataframeModeler, dataFrames: DataFrame*)(layers: Layer*)(implicit sparkSession: SparkSession): Result = {
    val (representationKeys, transformKeys) = ctx.initKeys(dataFrames: _*)
    val unitFeedback = new Tensor(1.0)
    val deltaResults = {
      val schema = (0 until dataFrames.size).map(i => {
        dataFrames(i).schema
      }).toArray
      zip(dataFrames.map(_.rdd): _*).collect().grouped(1000).map((itr: Array[Seq[Row]]) => {
        val list: List[Seq[Row]] = itr.toList
        val inputs: Seq[Result] = (0 until list.head.size).map(i => {
          ctx.convertToResults(schema(i), list.map(_.apply(i)))
        })
        val result = layers.foldLeft(inputs)((a: Seq[Result], b: Layer) => List(b.evalAndFree(a: _*))).head
        val map = evalFeedback(unitFeedback)(result)
        ctx.print(map, layers, representationKeys, transformKeys, "Partition")
        val tuple = (result.getData, map)
        tuple._1.detach()
        tuple._1.asInstanceOf[TensorArray].getData.foreach(_.detach())
        result.freeRef()
        tuple
      })
    }

    def sum(a: TensorList): Tensor = (0 until a.length()).map(a.get(_)).reduce(_.add(_))

    val summedResults: TensorList = deltaResults
      .map(_._1).reduce((a, b) => {
      val sumB = sum(b)
      val tensorArray = TensorArray.wrap(sum(a).addAndFree(sumB))
      sumB.freeRef()
      a.freeRef()
      b.freeRef()
      tensorArray
    })
    val uuidToDoubles: Map[UUID, Array[Double]] = deltaResults.toList
      .flatMap(_._2.toList).groupBy(_._1).mapValues(_.map(_._2).reduce(ArrayUtil.add(_, _)))
    ctx.print(uuidToDoubles, layers, representationKeys, transformKeys, "Collected")
    unitFeedback.freeRef()
    new Result(summedResults, new BiConsumer[DeltaSet[UUID], TensorList] {
      override def accept(buffer: DeltaSet[UUID], signal: TensorList): Unit = {
        if (signal.length() > 1) throw new IllegalArgumentException
        val accumulate = ctx.accumulate(buffer, layers, representationKeys, transformKeys) _
        uuidToDoubles.foreach(x => accumulate(x._1, x._2))
      }
    })
  }

}

class RDDModelingStrategy(defaultSize: Int*) extends ModelingStrategy(defaultSize:_*) {
  override def evalToDataframe(ctx: DataframeModeler, dataFrames: DataFrame*)(name: String, layers: Layer*)(implicit sparkSession: SparkSession): DataFrame = {
    val (representationKeys, transformKeys) = ctx.initKeys(dataFrames: _*)
    val schema = dataFrames.map(_.schema).toArray
    val ctxBroadcast = sparkSession.sparkContext.broadcast(ctx)
    val results = zip(dataFrames.map(_.rdd): _*).mapPartitions(itr => {
      val list = itr.toSeq
      val inputs: Seq[Result] = (0 until list.head.size).map(i => {
        val rows = list.map(_ (i))
        ctxBroadcast.value.convertToResults(schema(i), rows)
      })
      val result = layers.foldLeft(inputs)((a: Seq[Result], b: Layer) => List(b.evalAndFree(a: _*))).head.getDataAndFree
      (0 until result.length()).map(result.get(_)).iterator
    })
    sparkSession.createDataFrame(
      results.map((t: Tensor) => Row((0 until t.length()).map(i => t.get(i)).toList)),
      StructType(Array(
        StructField(name, ArrayType(DoubleType))
      ))
    )
  }

  override def eval(ctx: DataframeModeler, dataFrames: DataFrame*)(layers: Layer*)(implicit sparkSession: SparkSession): Result = {
    val (representationKeys, transformKeys) = ctx.initKeys(dataFrames: _*)
    val unitFeedback = new Tensor(1.0)
    def sum(tensorList: TensorList): Tensor = (0 until tensorList.length()).map(tensorList.get(_)).reduce((a,b)=>{
      val r = a.addAndFree(b)
      b.freeRef()
      r
    })
    val ctxBroadcast = sparkSession.sparkContext.broadcast(ctx)
    val (results, uuidToDoubles) = {
      val schema = (0 until dataFrames.size).map(i => {
        dataFrames(i).schema
      }).toArray
      zip(dataFrames.map(_.rdd): _*).mapPartitions(itr => {
        val list = itr.toSeq
        val inputs: Seq[Result] = (0 until list.head.size).map(i => {
          ctxBroadcast.value.convertToResults(schema(i), list.map(_.apply(i)))
        })
        val result = layers.foldLeft(inputs)((a: Seq[Result], b: Layer) => List(b.evalAndFree(a: _*))).head
        val map = evalFeedback(unitFeedback)(result)
        ctxBroadcast.value.print(map, layers, representationKeys, transformKeys, "Partition")
        val tuple = (sum(result.getDataAndFree), map)
        tuple._1.detach()
        List(tuple).iterator
      })
    }.reduce((a,b)=>{
      val (tensor1, map1) = a
      val (tensor2, map2) = b
      val tuple = List(tensor1, tensor2).reduce((a, b) => {
        val tensor = a.addAndFree(b)
        b.freeRef()
        tensor
      }) -> List(map1, map2).flatMap(_.toList).groupBy(_._1).mapValues(_.map(_._2).reduce(ArrayUtil.add(_, _)))
      tuple._1.detach()
      tuple
    })
    ctxBroadcast.unpersist()
    ctx.print(uuidToDoubles, layers, representationKeys, transformKeys, "Collected")
    unitFeedback.freeRef()
    new Result(TensorArray.wrap(results), new BiConsumer[DeltaSet[UUID], TensorList] {
      override def accept(buffer: DeltaSet[UUID], signal: TensorList): Unit = {
        if (signal.length() > 1) throw new IllegalArgumentException
        val accumulate = ctx.accumulate(buffer, layers, representationKeys, transformKeys) _
        uuidToDoubles.foreach(x => accumulate(x._1, x._2))
      }
    })
  }

}
