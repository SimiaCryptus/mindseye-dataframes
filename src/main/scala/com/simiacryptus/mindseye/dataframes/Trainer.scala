package com.simiacryptus.mindseye.dataframes

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
import com.simiacryptus.mindseye.dataframes.DataUtil._
import com.simiacryptus.mindseye.lang.ReferenceCountingBase
import com.simiacryptus.mindseye.layers.java.{EntropyLossLayer, FullyConnectedLayer, SoftmaxActivationLayer, SumMetaLayer}
import com.simiacryptus.mindseye.network.PipelineNetwork
import com.simiacryptus.mindseye.opt.IterativeTrainer
import com.simiacryptus.mindseye.opt.line.QuadraticSearch
import com.simiacryptus.mindseye.opt.orient.GradientDescent
import com.simiacryptus.notebook.NotebookOutput
import com.simiacryptus.sparkbook._
import com.simiacryptus.sparkbook.repl.{SparkRepl, SparkSessionProvider}
import com.simiacryptus.sparkbook.util.Java8Util._
import com.simiacryptus.sparkbook.util.Logging
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.storage.StorageLevel

abstract class Trainer extends SerializableFunction[NotebookOutput, Object] with Logging with SparkSessionProvider with InteractiveSetup[Object] {

  override def inputTimeoutSeconds = 600

  def dataSources: Map[String, String]

  def sourceTableName: String

  def avoidRdd = true

  final def sourceDataFrame = if (spark.sqlContext.tableNames().contains(sourceTableName)) spark.sqlContext.table(sourceTableName) else null

  def objectMapper = new ObjectMapper()
    .enable(SerializationFeature.INDENT_OUTPUT)
    .enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
    .enable(MapperFeature.USE_STD_BEAN_NAMING)
    .registerModule(DefaultScalaModule)
    .enableDefaultTyping()

  override def accept2(log: NotebookOutput): Object = {

    intercept(log, classOf[ReferenceCountingBase].getCanonicalName, additive = false)

    log.h1("Data Staging")
    log.p("""First, we will stage the initial data and manually perform a data staging query:""")
    val inputTables : List[DataFrame] = log.eval(() => {
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
        case IntegerType if("Cover_Type" == field.name) => field.name
        case IntegerType => s"CAST(${field.name} AS DOUBLE)"
        case _ => field.name
      }
    }).mkString(", \n\t")

    new SparkRepl() {

      override val defaultCmd: String =
        s"""%sql
           | CREATE TEMPORARY VIEW ${sourceTableName} AS
           | SELECT $selectStr FROM ${dataSources.values.head}
        """.stripMargin.trim

      override def shouldContinue(): Boolean = {
        sourceDataFrame == null
      }

    }.apply(log)

    log.p("""This sub-report can be used for concurrent adhoc data exploration:""")
    log.subreport("explore", (sublog: NotebookOutput) => {
      val thread = new Thread(() => {
        new SparkRepl().apply(sublog)
      }: Unit)
      thread.setName("Data Exploration REPL")
      thread.setDaemon(true)
      thread.start()
      null
    })

    val Array(trainingData, testingData) = sourceDataFrame.randomSplit(Array(0.1, 0.9))
    trainingData.persist(StorageLevel.MEMORY_ONLY_SER)
    log.h1("""Table Schema""")
    log.run(() => {
      sourceDataFrame.printSchema()
    })

    val frame: DataFrame = spark.sqlContext.emptyDataFrame
    frame.rdd.mapPartitions((iter: Iterator[Row]) => {
      List.empty.iterator
    })

    val classifierNetwork = new PipelineNetwork(1)
    classifierNetwork.add(new FullyConnectedLayer(Array(55), Array(10)))
    classifierNetwork.add(new SoftmaxActivationLayer())
    val lossNetwork = new PipelineNetwork(2)
    lossNetwork.add(new SumMetaLayer(),
      lossNetwork.add(new EntropyLossLayer(),
        lossNetwork.add(classifierNetwork, lossNetwork.getInput(0)),
        lossNetwork.getInput(1))
    )
    val model = DataframeModeler(size = (self, value) => {
      if (self.path == "Cover_Type") List(10)
      else List(1)
    })

    withMonitor(log) { trainingMonitor => {
      log.eval(() => {
        new IterativeTrainer(model.asTrainable(isLocal = avoidRdd)(
          trainingData.drop("Cover_Type"),
          trainingData.select("Cover_Type")
        )(
          lossNetwork
        ))
          .setMonitor(trainingMonitor)
          .setOrientation(new GradientDescent)
          .setLineSearchFactory((name: CharSequence) => new QuadraticSearch)
          .setTimeout(30, TimeUnit.MINUTES)
          .setMaxIterations(100)
          .runAndFree
          .toString
      })
    }
    }
  }


}



