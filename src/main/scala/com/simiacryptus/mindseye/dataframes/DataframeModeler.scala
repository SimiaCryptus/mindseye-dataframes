package com.simiacryptus.mindseye.dataframes

import java.nio.charset.Charset
import java.util.UUID

import com.google.common.hash.Hashing
import com.google.gson.JsonObject
import com.simiacryptus.mindseye.eval.ArrayTrainable
import com.simiacryptus.mindseye.lang._
import com.simiacryptus.mindseye.layers.ValueLayer
import com.simiacryptus.mindseye.network.DAGNetwork
import com.simiacryptus.sparkbook.util.Java8Util._
import com.simiacryptus.sparkbook.util.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

final case class DataframeModeler
(
  strategy: ModelingStrategy,
  path: String = "",
  ctx: ModelingData = new ModelingData
) extends Logging {


  def evalToDataframe(dataFrames: DataFrame*)(name: String, layers: Layer*)(implicit sparkSession: SparkSession): DataFrame = strategy.evalToDataframe(this,dataFrames:_*)(name,layers:_*)

  def eval(dataFrames: DataFrame*)(layers: Layer*)(implicit sparkSession: SparkSession): Result = strategy.eval(this,dataFrames:_*)(layers:_*)

  def child(name: String): DataframeModeler = copy(path = List(path, name).filterNot(_.isEmpty).mkString("/"))

  private def valueStr(value: Any) = {
    require(null != value)
    this.path + "=" + value
  }

  def getRepresentation(key: String) = {
    require(null != key)
    ctx.representations.synchronized {
      ctx.representations.getOrElseUpdate(key, this.strategy.initialRepresentation(key))
    }
  }

  def getTransform(key: String, stats: => (Double, Double, Double)) = {
    require(null != key)
    ctx.transforms.synchronized {
      ctx.transforms.getOrElseUpdate(key, this.strategy.initialTransform(key, stats))
    }
  }

  def getRepresentationKeys(field: DataType, data: Seq[_]): Seq[String] = {
    field match {
      case struct: StructType =>
        struct.fields.zipWithIndex.flatMap(t => {
          val (f, i) = t
          child(f.name).getRepresentationKeys(f.dataType, data.map(_.asInstanceOf[Row].get(i)))
        }).distinct
      case _: IntegerType =>
        data.map(value => valueStr(value.toString)).distinct
      case _: StringType =>
        data.map(value => valueStr(value.toString)).distinct
      case _ =>
        Seq.empty
    }
  }

  def getTransformKeys(field: DataType, data: Seq[_]): Seq[(String, Array[Double])] = {
    field match {
      case struct: StructType =>
        struct.fields.zipWithIndex.flatMap(t => {
          val (f, i) = t
          child(f.name).getTransformKeys(f.dataType, data.map(_.asInstanceOf[Row].get(i)))
        })
      case f: DoubleType =>
        Seq(path -> getMoments(data.map({
          case d: Number => d.doubleValue()
          case o => o.toString.toDouble
        }): _*)
        )
      case _ =>
        Seq.empty
    }
  }

  def convertToResults(field: DataType, data: => Seq[_]): Result = {
    field match {
      case struct: StructType =>
        strategy.reduceStruct(this, struct.fields.zipWithIndex.map(t => {
          val (f, i) = t
          child(f.name).convertToResults(f.dataType, data.map(_.asInstanceOf[Row].get(i)))
        }))
      case _: DoubleType =>
        transformScalars(data.map({
          case n: Number => n.doubleValue()
          case s: Any => s.toString.toDouble
        }))
      case _: IntegerType =>
        evaluateRepresentations(data)
      case _: StringType =>
        evaluateRepresentations(data)
    }
  }

  def transformScalars(doubles: Seq[Double]) = {
    getTransform(path, getStats(doubles: _*)).evalAndFree(new ConstantResult(TensorArray.wrap(doubles.map(new Tensor(_)).toArray: _*)))
  }

  def getStats(doubles: Double*) = {
    momentsToStats(getMoments(doubles: _*))
  }

  def momentsToStats(moments: Array[Double]) = {
    val mean = moments(1) / moments(0)
    val stdDev = Math.sqrt(Math.abs(mean * mean - (moments(2) / moments(0))))
    (moments(0), mean, stdDev)
  }

  def getMoments(doubles: Double*) = {
    doubles.map(x => (0 to 2).map(i => Math.pow(x, i))).reduce(_.zip(_).map(t => t._1 + t._2)).toArray
  }

  def evaluateRepresentations(values: Seq[Any]) = {
    require(null != values)
    val layer = new ValueLayer(values.map(value => {
      getRepresentation(this.valueStr(value))
    }): _*).setFrozen(false)
    val result = strategy.edit(this, layer).eval(Array.empty[Tensor]: _*)
    layer.freeRef()
    result
  }

  def asTrainable(dataFrames: DataFrame*)(layers: Layer*)(implicit sparkSession: SparkSession) = {
    new ArrayTrainable(Array(Array(new Tensor(1))), new LayerBase() {
      override def getJson(resources: java.util.Map[CharSequence, Array[Byte]], dataSerializer: DataSerializer): JsonObject = throw new RuntimeException()

      override def state(): java.util.List[Array[Double]] = java.util.Arrays.asList()

      override def eval(array: Result*): Result = strategy.eval(DataframeModeler.this, dataFrames: _*)(layers: _*)
    }, 1)
  }

  def zipLocal[T: ClassTag](seq: Seq[T]*): Seq[Seq[T]] = {
    seq.map(_.map(Seq(_))).reduce(_.zip(_).map(t => t._1 ++ t._2))
  }

  def initKeys(dataFrames: DataFrame*) = {
    val representationKeys = dataFrames.flatMap(data => {
      val schema = data.schema
      data.rdd.mapPartitions((rows: Iterator[Row]) => {
        DataframeModeler.this.getRepresentationKeys(schema, rows.toSeq).iterator
      }).distinct().collect()
    }).distinct.sorted.toList
    for (representationKey <- representationKeys) getRepresentation(representationKey)

    val transformKeys = dataFrames.map(data => {
      val schema = data.schema
      data.rdd.mapPartitions(rows => DataframeModeler.this.getTransformKeys(schema, rows.toSeq).iterator)
    }).reduce(_.union(_)).groupBy(_._1).mapValues(_.map(_._2).reduce(_.zip(_).map(t => t._1 + t._2))).sortBy(_._1).collect()
    for (transformKey <- transformKeys) getTransform(transformKey._1, getStats(transformKey._2: _*))
    (representationKeys, transformKeys.map(_._1).distinct)
  }

  def print(uuidToDoubles: Map[UUID, Array[Double]], layers: Seq[Layer], keys: Seq[String], transformKeys: Seq[String], header: String) = {
    uuidToDoubles.foreach(t => {
      def uuid = t._1
      def name = findLayer(layers, uuid).map(_.getName)
        .orElse(uuidMap(keys ++ transformKeys).get(uuid))
        .getOrElse("???")
      logger.debug(s"$header Delta: $uuid -> $name = ${t._2.mkString(",")}")
    })
  }

  def accumulate(buffer: DeltaSet[UUID], layers: Seq[Layer], representationKeys: List[String], transformKeys: Seq[String])(uuid: UUID, delta: Array[Double]) = {
    findLayer(layers, uuid).map(localLayer => {
      buffer.get(localLayer.getId, localLayer.state().get(0)).addInPlace(delta).freeRef()
    }).orElse(uuidMap(representationKeys).get(uuid).map(id => {
      buffer.get(uuid, getRepresentation(id)).addInPlace(delta).freeRef()
    })).orElse(uuidMap(transformKeys).get(uuid).map(id => {
      buffer.get(uuid, getTransform(id, throw new RuntimeException()).state().get(0)).addInPlace(delta).freeRef()
    })).getOrElse({
      logger.info("No match found for " + uuid, new RuntimeException("Stack Trace"))
    })
  }

  def findLayer(layers: Seq[Layer], uuid: UUID): Option[Layer] = {
    layers.flatMap({
      case net: Layer if (net.getId.equals(uuid)) => Option(net)
      case net: DAGNetwork =>
        findLayer(net.getLayersById.values().asScala.toSeq, uuid)
      case _ => None
    }).headOption
  }

  def uuidMap(keys: Seq[String]): Map[UUID, String] = {
    keys.map(id => UUID.nameUUIDFromBytes((id).getBytes("UTF-8")) -> id).toMap
  }
}

object DataframeModeler {
  val seedKey = getClass.getSimpleName.getBytes

  def evalFeedback(feedback: Tensor): Result => Map[UUID, Array[Double]] = (remoteResult: Result) => {
    toMap(toDelta(remoteResult, feedback))
  }

  def zip(o: DataFrame*)(implicit sparkSession: SparkSession): DataFrame = {
    val rows = zip(o.map(_.rdd): _*).map(t => Row(t.map(_.toSeq).reduce(_ ++ _): _*))
    val schema = StructType(o.map(_.schema.toSeq).reduce(_ ++ _))
    sparkSession.createDataFrame(rows, schema)
  }

  def zip[T: ClassTag](o: RDD[T]*): RDD[Seq[T]] = {
    def fn[U: ClassTag](x: RDD[U]): RDD[(Long, U)] = x.zipWithIndex().map(x => x._2 -> x._1)

    o.map(_.map(Seq(_))).reduce((a, b) => {
      fn(a).join(fn(b)).map(_._2).map((x) => x._1 ++ x._2)
    })
  }

  def toMap(deltaSet: DeltaSet[UUID]) = {
    val list = deltaSet.getMap.asScala.flatMap({
      case (layer: UUID, delta: Delta[UUID]) =>
        Option(layer -> delta.getDelta)
    }).toList
    deltaSet.freeRef()
    list.toMap
  }

  def toDelta(remoteResult: Result, feedback: Tensor) = {
    val deltaSet = new DeltaSet[UUID]()
    val tensorArray = TensorArray.create(Array.fill(remoteResult.getData.length())(feedback): _*)
    remoteResult.accumulate(deltaSet, tensorArray)
    deltaSet
  }

  def rawHash(str: String) = {
    val function = Hashing.hmacSha1(seedKey)
    val hashResult = function.hashString(str, Charset.forName("UTF-8"))
    hashResult
  }

}