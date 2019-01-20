package com.simiacryptus.mindseye.tensorflow

import com.simiacryptus.sparkbook.NotebookRunner
import com.simiacryptus.sparkbook.util.LocalRunner

object CovType_Trainer_Local extends CovType_Trainer with LocalRunner[Object] with NotebookRunner[Object]
