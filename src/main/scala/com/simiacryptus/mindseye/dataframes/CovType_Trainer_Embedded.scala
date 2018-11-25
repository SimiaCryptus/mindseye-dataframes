package com.simiacryptus.mindseye.dataframes

import com.simiacryptus.sparkbook.{EmbeddedSparkRunner, NotebookRunner}

object CovType_Trainer_Embedded extends CovType_Trainer with EmbeddedSparkRunner[Object] with NotebookRunner[Object] {

  override protected val s3bucket: String = envTuple._2

  override def numberOfWorkersPerNode: Int = 2

  override def workerMemory: String = "2g"

}
