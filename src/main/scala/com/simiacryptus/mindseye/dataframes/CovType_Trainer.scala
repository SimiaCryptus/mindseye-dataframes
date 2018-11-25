package com.simiacryptus.mindseye.dataframes

import org.apache.spark.sql.types.StructType

abstract class CovType_Trainer extends Trainer {

  override val dataSources = Map(
    "s3a://simiacryptus/data/covtype/" -> "src_covtype"
  )
  val target = Array("Cover_Type")
  val sourceTableName: String = "covtype"
  val supervision: String = "supervised"


}
