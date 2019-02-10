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

import java.io.{File, IOException, OutputStream, PrintWriter}
import java.text.SimpleDateFormat
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.BiConsumer
import java.util.{Date, UUID}

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.{Level, Logger}
import ch.qos.logback.core.AppenderBase
import com.simiacryptus.mindseye.lang._
import com.simiacryptus.mindseye.opt.{Step, TrainingMonitor}
import com.simiacryptus.mindseye.test.{StepRecord, TestUtil}
import com.simiacryptus.notebook.{MarkdownNotebookOutput, NotebookOutput}
import com.simiacryptus.sparkbook.util.Java8Util._
import com.simiacryptus.sparkbook.util.Logging
import com.simiacryptus.util.Util
import javax.imageio.ImageIO
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.{Duration, _}
object DataUtil extends Logging {

  private def now: Long = System.currentTimeMillis()
  private case class LogWriterState
  (
    file: PrintWriter,
    time: Long = now,
    counter: AtomicInteger = new AtomicInteger(0)
  )
  def intercept(log: NotebookOutput, loggerName: String, maxSize: Int = 1000000, maxDuration: Duration = 1 hour, level: Level = Level.ALL, additive: Boolean = true): Unit = {

    log.subreport("log_" + loggerName, (sublog:NotebookOutput) => {
      def newOut = {
        val name = loggerName + "_" + new SimpleDateFormat("dd_HH_mm").format(new Date) + ".log"
        sublog.out(sublog.link(new File(sublog.getResourceDir, name), name))
        new PrintWriter(sublog.file(name))
      }
      val logger = LoggerFactory.getLogger(loggerName).asInstanceOf[Logger]
      logger.setLevel(level)
      logger.setAdditive(additive)

      val appender = new AppenderBase[ILoggingEvent] {
        var state = LogWriterState(file = newOut)

        def current(size: Int) = {
          if (state.time < (now - maxDuration.toMillis) || state.counter.addAndGet(size) > maxSize) {
            state.file.close()
            state = LogWriterState(file = newOut)
          }
          state.file
        }

        override def append(e: ILoggingEvent): Unit = {
          val formattedMessage = e.getFormattedMessage
          val writer = current(formattedMessage.length)
          writer.println(formattedMessage)
          writer.flush()
        }
      }
      appender.start()
      logger.addAppender(appender)
      null
    })
  }


  def withMonitor[T](log: NotebookOutput)(fn: TrainingMonitor => T) = {

    val history = new ArrayBuffer[StepRecord]()
    val trainingMonitor = new TrainingMonitor {
      override def log(msg: String): Unit = {
        logger.info(msg)
        super.log(msg)
      }

      override def onStepComplete(currentPoint: Step): Unit = {
        history += new StepRecord(currentPoint.point.getMean, currentPoint.time, currentPoint.iteration)
      }
    }
    val training_name = String.format("etc/training_plot_%s.png", java.lang.Long.toHexString(MarkdownNotebookOutput.random.nextLong))
    log.p(String.format("<a href=\"%s\"><img src=\"%s\"></a>", training_name, training_name))
    val closeable = log.getHttpd.addGET(training_name, "image/png", (r: OutputStream) => {
      try {
        val image1 = Util.toImage(TestUtil.plot(history))
        if (null != image1) ImageIO.write(image1, "png", r)
      } catch {
        case e: IOException =>
          logger.warn("Error writing result images", e)
      }
    }: Unit)
    try {
      fn.apply(trainingMonitor)
    } finally {
      try {
        closeable.close()
        val image = Util.toImage(TestUtil.plot(history))
        if (null != image) ImageIO.write(image, "png", log.file(training_name))
      } catch {
        case e: IOException =>
          logger.warn("Error writing result images", e)
      }
    }
  }
  class ConcatResult(val children: Result*) extends Result(
    children.map(_.getData).reduce(concatTensorList(_,_)),
    new BiConsumer[DeltaSet[UUID], TensorList] {
      override def accept(buffer: DeltaSet[UUID], signal: TensorList): Unit = {
        children.foldLeft(0)((l,b)=>{
          val n = b.getData.length()
          b.accumulate(buffer, selectTensorList(signal, l, n))
          n+l
        })
      }
    }) {
//    val childData = children.map(x=>{
//      val data = x.getData
//      data.addRef()
//      data
//    })

    override protected def _free(): Unit = {
//      childData.foreach(_.freeRef())
      children.foreach(_.freeRef())
      super._free()
    }

  }

  def concatAndFree(a: Result, b: Result): Result = {
    (a,b) match {
      case (a: ConcatResult, b: ConcatResult) => {
        a.children.foreach(_.addRef())
        b.children.foreach(_.addRef())
        val concatResult = new ConcatResult((a.children ++ b.children):_*)
        a.freeRef()
        b.freeRef()
        concatResult
      }
      case (a: Result, b: Result) =>
        new ConcatResult(a,b)
    }
  }

  def selectTensorList(aData: TensorList, offset: Int, selectionLength: Int): TensorList = {
    new ReferenceCountingBase with TensorList {
      val aLength = aData.length()
      aData.addRef()

      override def get(i: Int): Tensor = {
        aData.get(i - offset)
      }

      override def getDimensions: Array[Int] = aData.getDimensions

      override def length(): Int = selectionLength

      override def stream(): java.util.stream.Stream[Tensor] = {
        aData.stream().skip(offset).limit(selectionLength)
      }

      override protected def _free(): Unit = {
        aData.freeRef()
        super._free()
      }
    }
  }


  def concatTensorList(aData: TensorList, bData: TensorList): TensorList = {
    new ReferenceCountingBase with TensorList {
      val aLength = aData.length()
      val bLength = bData.length()
      aData.addRef()
      bData.addRef()

      override def get(i: Int): Tensor = {
        if (i < aLength) {
          aData.get(i)
        } else {
          bData.get(i - aLength)
        }
      }

      override def getDimensions: Array[Int] = aData.getDimensions

      override def length(): Int = aLength + bLength

      override def stream(): java.util.stream.Stream[Tensor] = java.util.stream.Stream.concat(
        aData.stream(),
        bData.stream()
      )

      override protected def _free(): Unit = {
        aData.freeRef()
        bData.freeRef()
        super._free()
      }
    }
  }
}
