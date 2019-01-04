package net.degols.libs.filesgate.core.messagedistributor

import net.degols.libs.filesgate.core.PipelineStepStatus
import net.degols.libs.filesgate.core.pipelineinstance.PipelineStepMessageWrapper
import net.degols.libs.filesgate.utils.Step

import scala.util.Random

class BasicMessageDistributor extends MessageDistributor {
  override def bestPipelineStepStatus(m: PipelineStepMessageWrapper, targetedStep: Step, stepStatuses: List[PipelineStepStatus]): Option[PipelineStepStatus] = {
    Random.shuffle(stepStatuses).headOption
  }
}
