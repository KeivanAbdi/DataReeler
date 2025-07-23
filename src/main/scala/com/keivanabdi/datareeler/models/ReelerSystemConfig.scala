package com.keivanabdi.datareeler.models

import org.apache.pekko.util.Timeout

import scala.concurrent.duration.Duration

import com.keivanabdi.datareeler.templates.ReelerTemplate

case class ReelerSystemConfig[D, MD, TI](
    reelerTemplate                : ReelerTemplate[D, MD, TI],
    initialMetaData               : () => MD,
    demandBatchSize               : Int,
    timeout                       : Timeout,
    dataBufferSize                : Int    = 128,
    metaDataBufferSize            : Int    = 128,
    templateInstructionsBufferSize: Int    = 128,
    demandBufferSize              : Int    = 128,
    interface                     : String = "0.0.0.0",
    port                          : Int    = 8080
)
