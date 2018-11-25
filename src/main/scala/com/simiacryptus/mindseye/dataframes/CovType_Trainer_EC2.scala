package com.simiacryptus.mindseye.dataframes

import com.simiacryptus.aws.exe.EC2NodeSettings
import com.simiacryptus.sparkbook.{AWSNotebookRunner, EC2SparkRunner}

object CovType_Trainer_EC2 extends CovType_Trainer with EC2SparkRunner[Object] with AWSNotebookRunner[Object] {

  protected override val s3bucket: String = envTuple._2

  override def numberOfWorkerNodes: Int = 1

  override def numberOfWorkersPerNode: Int = 1

  override def workerCores: Int = 8

  override def driverMemory: String = "14g"

  override def workerMemory: String = "14g"

  override def masterSettings: EC2NodeSettings = EC2NodeSettings.M5_XL

  override def workerSettings: EC2NodeSettings = EC2NodeSettings.M5_XL

}
