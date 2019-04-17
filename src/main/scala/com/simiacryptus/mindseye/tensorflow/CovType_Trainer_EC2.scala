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

import com.simiacryptus.aws.exe.EC2NodeSettings
import com.simiacryptus.sparkbook.{AWSNotebookRunner, EC2SparkRunner}

object CovType_Trainer_EC2 extends CovType_Trainer with EC2SparkRunner[Object] with AWSNotebookRunner[Object] {

  override val s3bucket: String = envTuple._2

  //  override def numberOfWorkerNodes: Int = 1
  //
  //  override def numberOfWorkersPerNode: Int = 1
  //
  //  override def workerCores: Int = 8
  //
  //  override def driverMemory: String = "14g"
  //
  //  override def workerMemory: String = "14g"
  override val workerMemory: String = "14g"

  override def hiveRoot: Option[String] = None

  override def masterSettings: EC2NodeSettings = EC2NodeSettings.M5_XL

  override def workerSettings: EC2NodeSettings = EC2NodeSettings.M5_XL

}
