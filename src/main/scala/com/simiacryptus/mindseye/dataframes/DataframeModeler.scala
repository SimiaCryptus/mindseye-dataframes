package com.simiacryptus.mindseye.dataframes

import java.nio.charset.Charset
import java.util.function.BiConsumer
import java.util.{Random, UUID}

import com.google.common.hash.Hashing
import com.google.gson.JsonObject
import com.simiacryptus.mindseye.dataframes.DataframeModeler._
import com.simiacryptus.mindseye.eval.ArrayTrainable
import com.simiacryptus.mindseye.lang._
import com.simiacryptus.mindseye.layers.java.{FullyConnectedLayer, TensorConcatLayer, ValueLayer}
import com.simiacryptus.mindseye.network.DAGNetwork
import com.simiacryptus.sparkbook.util.Java8Util._
import com.simiacryptus.sparkbook.util.Logging
import com.simiacryptus.util.ArrayUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataType, IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.JavaConverters._
import scala.collection.{immutable, mutable}

final case class DataframeModeler
(
  size: (DataframeModeler, Any) => immutable.Seq[Int],
  path: String = "",
  representationVectors: mutable.HashMap[String, Tensor] = new mutable.HashMap[String, Tensor]()
) extends Logging {
  def child(name: String): DataframeModeler = copy(path = List(path, name).filterNot(_.isEmpty).mkString("/"))

  def initLayer(value: String): Tensor = {
    require(null != value)
    val id = UUID.nameUUIDFromBytes(value.getBytes("UTF-8"))
    //logger.info(s"Initialize value for $value = $id")
    val hashCode = hash(value)
    val seeded = new Random(hashCode.asLong())
    val initData = new Tensor(DataframeModeler.this.size(this, value): _*)
    initData.set(() => (seeded.nextDouble() * 2 - 1) * 0.001)
    initData.setId(id)
    require(null != initData)
    initData
  }


  def hash(value: String) = {
    DataframeModeler.rawHash(valueStr(value))
  }


  private def valueStr(value: Any) = {
    require(null != value)
    this.path + "=" + value
  }

  def getResult(values: Seq[Any]): (Result, Seq[(String, String)]) = {
    require(null != values)
    val tensors = values.map(value => {
      get(valueStr(value))
    })
    new ValueLayer(tensors: _*).eval() -> values.map(value => path -> value.toString)
  }

  def get(key: String) = {
    require(null != key)
    representationVectors.synchronized {
      val tensor = representationVectors.getOrElseUpdate(key, this.initLayer(key))
      require(null != tensor)
      tensor
    }
  }


  def convert(field: DataType, data: Seq[_]): (Result, Seq[(String, String)]) = {
    (field, data) match {
      case (struct: StructType, row: Seq[_]) =>
        val tuples: Array[(Result, Seq[(String, String)])] = struct.fields.map(f => {
          child(f.name).convert(f.dataType, row.map(_.asInstanceOf[Row].getAs[Object](f.name)))
        })
        new TensorConcatLayer().eval(tuples.map(_._1): _*) -> tuples.flatMap(_._2)
      case (_: IntegerType, row: Seq[_]) =>
        this.getResult(row)
      case (_: StringType, row: Seq[_]) =>
        this.getResult(row)
    }
  }

  def asTrainable(isLocal:Boolean=false)(dataFrames: DataFrame*)(layers: Layer*) = {
    new ArrayTrainable(Array(Array(new Tensor(1))), new LayerBase() {
      override def getJson(resources: java.util.Map[CharSequence, Array[Byte]], dataSerializer: DataSerializer): JsonObject = throw new RuntimeException()

      override def state(): java.util.List[Array[Double]] = java.util.Arrays.asList()

      override def eval(array: Result*): Result = if(isLocal) DataframeModeler.this.evalLocal(dataFrames: _*)(layers: _*) else DataframeModeler.this.eval(dataFrames: _*)(layers: _*)
    }, 1)
  }

  def evalLocal(dataFrames: DataFrame*)(layers: Layer*) = {
    val translated: Seq[(Result, Seq[(String, String)])] = dataFrames.map(data => {
      val schema = data.schema
      DataframeModeler.this.convert(schema, data.rdd.collect())
    })
    val keys = translated.flatMap(_._2).distinct.sorted.toList
    val postNetwork = layers.foldLeft(translated.map(_._1))((a: Seq[Result], b: Layer) => {
      val result = b.eval(a: _*)
      a.foreach(_.freeRef())
      Seq(result)
    }).head
    require(postNetwork.getData.length() == 1)
    new Result(postNetwork.getData, new BiConsumer[DeltaSet[UUID], TensorList] {
      override def accept(buffer: DeltaSet[UUID], signal: TensorList): Unit = {
        if (signal.length() > 1) throw new IllegalArgumentException(s"signal.length() = ${signal.length()}")
        evalFeedback(signal.get(0))(postNetwork).foreach(accumulate(buffer, layers, keys)(_))
      }
    })
  }

  def eval(dataFrames: DataFrame*)(layers: Layer*) = {
    val translated: Seq[RDD[(Result, Seq[(String, String)])]] = dataFrames.map(data => {
      val schema = data.schema
      data.rdd.mapPartitions(rows => {
        List(DataframeModeler.this.convert(schema, rows.toList)).iterator
      }).cache()
    })
    val keys = translated.flatMap(_.flatMap(_._2).distinct().collect()).distinct.sorted.toList
    val postNetwork: RDD[Result] = translated.map(_.map(_._1)).map(_.map(t => List(t))).reduce(_.zip(_).map(t => t._1 ++ t._2)).map(inputs => {
      layers.foldLeft(inputs)((a: Seq[Result], b: Layer) => List(b.eval(a: _*))).head
    }).cache()

    def sum(a: TensorList): Tensor = (0 until a.length()).map(a.get(_)).reduce(_.add(_))

    val summedResults: TensorList = postNetwork.map(_.getData).reduce((a, b) => TensorArray.create(sum(a).add(sum(b))))
    translated.foreach(_.unpersist())
    new Result(summedResults, new BiConsumer[DeltaSet[UUID], TensorList] {
      override def accept(buffer: DeltaSet[UUID], signal: TensorList): Unit = {
        if (signal.length() > 1) throw new IllegalArgumentException
        postNetwork.flatMap(evalFeedback(signal.get(0))).groupBy(_._1).mapValues(_.map(_._2).reduce(ArrayUtil.add(_, _))).collect().foreach(accumulate(buffer, layers, keys))
      }
    }) {
      override protected def _free(): Unit = {
        postNetwork.unpersist()
        super._free()
      }
    }
  }

  def accumulate(buffer: DeltaSet[UUID], layers: Seq[Layer], keys: List[(String, String)])(t: (UUID, Array[Double])) = {

    val (uuid: UUID, delta: Array[Double]) = t
    layers.flatMap(l => {
      (l match {
        case net: Layer if (net.getId.equals(uuid)) => Option(net)
        case net: DAGNetwork =>
          net.getLayersById.asScala.filter(t => t._1 == uuid).values.headOption
      }).map(localLayer => {
        buffer.get(localLayer.getId, localLayer.state().get(0)).addInPlace(delta)
      })
    }).headOption.getOrElse(
      keys.flatMap(id => {
        if (UUID.nameUUIDFromBytes((id._1 + "=" + id._2).getBytes("UTF-8")) == uuid) {
          logger.info("Increment value for " + id._1 + " = " + id._2)
          Option(id)
        } else {
          None
        }
      }).map(id => {
        buffer.get(uuid, get(id._1 + "=" + id._2)).addInPlace(delta)
      }).headOption.getOrElse({
        logger.info("No match found for " + uuid)
      }))

  }
}

object DataframeModeler {
  val seedKey = getClass.getSimpleName.getBytes

  def evalFeedback(feedback: Tensor): Result => Map[UUID, Array[Double]] = (remoteResult: Result) => {
    val deltaSet = new DeltaSet[UUID]()
    val tensors = Array.fill(remoteResult.getData.length())(feedback)
    remoteResult.accumulate(deltaSet, TensorArray.create(tensors: _*))
    val list = deltaSet.getMap.asScala.flatMap({
      case (layer: UUID, delta: Delta[UUID]) =>
        Option(layer -> delta.target)
    }).toList
    deltaSet.freeRef()
    list.toMap
  }

  def rawHash(str: String) = {
    val function = Hashing.hmacSha1(seedKey)
    val hashResult = function.hashString(str, Charset.forName("UTF-8"))
    hashResult
  }

}