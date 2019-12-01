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

/*
 * Copyright (c) 2018 by Andrew Charneski.
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

import java.util.concurrent.TimeUnit

import com.fasterxml.jackson.databind.{MapperFeature, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.simiacryptus.lang.SerializableFunction
import com.simiacryptus.mindseye.lang.Layer
import com.simiacryptus.mindseye.layers.java._
import com.simiacryptus.mindseye.network.PipelineNetwork
import com.simiacryptus.mindseye.opt.IterativeTrainer
import com.simiacryptus.mindseye.opt.line.{BisectionSearch, LineSearchCursor, LineSearchStrategy}
import com.simiacryptus.mindseye.opt.orient.{GradientDescent, OrientationStrategy}
import com.simiacryptus.mindseye.tensorflow.DataUtil._
import com.simiacryptus.notebook.NotebookOutput
import com.simiacryptus.ref.lang.ReferenceCountingBase
import com.simiacryptus.sparkbook._
import com.simiacryptus.sparkbook.repl.{SparkRepl, SparkSessionProvider}
import com.simiacryptus.sparkbook.util.Java8Util._
import com.simiacryptus.sparkbook.util.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.storage.StorageLevel

abstract class Trainer extends SerializableFunction[NotebookOutput, Object] with Logging with SparkSessionProvider with InteractiveSetup[Object] {

  val categories = 7
  val featureDim = 10
  val activationClass = classOf[ReLuActivationLayer].getCanonicalName
  val driveClass = classOf[BisectionSearch].getCanonicalName
  val steeringClass = classOf[GradientDescent].getCanonicalName
  val midLayers: List[Int] = List(200, 200)
  val trainingSchedule = List(0.005, 0.01, 0.01, 0.05, 0.05, 0.1)
  val categoryColumnName = "Cover_Type"

  def dataSources: Map[String, String]

  def sourceTableName: String

  def objectMapper = new ObjectMapper()
    .enable(SerializationFeature.INDENT_OUTPUT)
    .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
    .enable(MapperFeature.USE_STD_BEAN_NAMING)
    .registerModule(DefaultScalaModule)
    .enableDefaultTyping()

  override def postConfigure(log: NotebookOutput): Object = {
    implicit val _log = log
    intercept(log, classOf[ReferenceCountingBase].getCanonicalName, additive = false)

    log.h1("Data Staging")
    log.p("""First, we will stage the initial data and manually perform a data staging query:""")
    val inputTables: List[DataFrame] = log.eval(() => {
      dataSources.map(t => {
        val (k, v) = t
        val frame = spark.sqlContext.read.parquet(k).persist(StorageLevel.DISK_ONLY)
        frame.createOrReplaceTempView(v)
        println(s"Loaded ${frame.count()} rows to ${v}")
        frame
      }).toList
    })
    val selectStr = inputTables.head.schema.fields.map(field => {
      field.dataType match {
        case _ if field.name.startsWith("Soil_Type") => ""
        case _ if "Cover_Type" == field.name => field.name
        case IntegerType => s"CAST(${field.name} AS DOUBLE)"
        case _ => field.name
      }
    }).filterNot(_.isEmpty).mkString(", \n\t")

    implicit val sparkSession = spark
    new SparkRepl() {
      override val inputTimeout = inputTimeoutSeconds
      override val defaultCmd: String =
        s"""%sql
           | CREATE TEMPORARY VIEW ${sourceTableName} AS
           | SELECT $selectStr FROM ${dataSources.values.head}
        """.stripMargin.trim

      override def shouldContinue(): Boolean = {
        sourceDataFrame == null
      }
    }.apply(log)

    //    log.p("""This sub-report can be used for concurrent adhoc data exploration:""")
    //    log.subreport("explore", (sublog: NotebookOutput) => {
    //      val thread = new Thread(() => {
    //        new SparkRepl().apply(sublog)
    //      }: Unit)
    //      thread.setName("Data Exploration REPL")
    //      thread.setDaemon(true)
    //      thread.start()
    //      null
    //    })

    def activation = Class.forName(activationClass).asInstanceOf[Class[Layer]].newInstance()

    def drive = Class.forName(driveClass).asInstanceOf[Class[LineSearchStrategy]].newInstance()

    def steering = Class.forName(steeringClass).asInstanceOf[Class[OrientationStrategy[LineSearchCursor]]].newInstance()

    val strategy = new CategorizingModelingStrategy(categoryColumnName, categories, featureDim)
    val model = DataframeModeler(strategy)
    sourceDataFrame.persist(StorageLevel.MEMORY_ONLY_SER)
    log.h1("""Table Schema""")
    log.run(() => {
      sourceDataFrame.printSchema()
    })

    val inputDim = Array(strategy.evalToDataframe(model, sourceDataFrame.drop(categoryColumnName).limit(1))("raw").rdd.collect().head.getAs[Seq[_]](0).length)

    val classifierNetwork = new PipelineNetwork(1)
    val finalDim = midLayers.foldLeft(inputDim)((a, b) => {
      classifierNetwork.add(new FullyConnectedLayer(a, Array(b)))
      classifierNetwork.add(new BiasLayer(b))
      classifierNetwork.add(activation)
      Array(b)
    })
    classifierNetwork.add(new FullyConnectedLayer(finalDim, Array(categories)))
    classifierNetwork.add(new BiasLayer(categories))
    classifierNetwork.add(new SoftmaxLayer())

    val lossNetwork = new PipelineNetwork(2)
    lossNetwork.add(classifierNetwork)
    lossNetwork.add(new BoundedActivationLayer().setMaxValue(0.9))
    lossNetwork.add(new AvgMetaLayer(),
      lossNetwork.add(new EntropyLossLayer(),
        lossNetwork.getHead,
        lossNetwork.getInput(1))
    )

    val Array(trainingData, testingData) = sourceDataFrame.randomSplit(Array(0.9, 0.1))
    trainingData.cache()
    for (trainingBatch <- trainingSchedule.map(f => trainingData.sample(f).repartition(Math.max((trainingData.count() * f / 10000).toInt, 2)))) {
      withMonitor(log) { trainingMonitor => {
        trainingBatch.persist(StorageLevel.MEMORY_ONLY_SER)
        log.eval(() => {
          new IterativeTrainer(model.asTrainable(
            trainingBatch.drop(categoryColumnName),
            trainingBatch.select(categoryColumnName)
          )(
            lossNetwork
          ))
            .setMonitor(trainingMonitor)
            .setOrientation(steering)
            .setLineSearchFactory((_: CharSequence) => drive)
            .setTimeout(30, TimeUnit.MINUTES)
            .setMaxIterations(10)
            .runAndFree
            .toString
        })
        trainingBatch.unpersist()
      }
      }
    }

    val testingData2 = testingData.limit(100)
    SparkRepl.out(DataframeModeler.zip(
      testingData2.select(categoryColumnName),
      testingData2.drop(categoryColumnName),
      model.evalToDataframe(testingData2.drop(categoryColumnName))("Prediction", classifierNetwork)
    ).cache())

    null
  }

  override def inputTimeoutSeconds = 1

  final def sourceDataFrame = if (spark.sqlContext.tableNames().contains(sourceTableName)) spark.sqlContext.table(sourceTableName) else null


}



