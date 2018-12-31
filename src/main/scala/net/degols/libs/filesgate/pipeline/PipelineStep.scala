package net.degols.libs.filesgate.pipeline

import net.degols.libs.filesgate.core.EngineInternalMessage
import net.degols.libs.filesgate.orm.FileMetadata
import net.degols.libs.filesgate.utils.Step

trait PipelineStep {
  val TYPE: String = "Unknown"
  val IMPORTANT_STEP: Boolean = false

  val DEFAULT_STEP_NAME: String = "Unknown"
}

/**
  * @param reason the reason why we aborted the next step
  * @param rescheduleSeconds if the value is filled, it means we do not want to processdownload the file right now, but in
  *                          x seconds. The message won't go to any next processing step. If the value is negative,
  *                          we will never re-schedule it.
  */
@SerialVersionUID(0L)
case class AbortStep(reason: String, rescheduleSeconds: Option[Long])


@SerialVersionUID(0L)
abstract class PipelineStepMessage(val fileMetadata: FileMetadata, val abort: Option[AbortStep]) extends EngineInternalMessage
