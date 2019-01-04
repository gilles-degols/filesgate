package net.degols.libs.filesgate.core.messagedistributor

import net.degols.libs.filesgate.core.PipelineStepStatus
import net.degols.libs.filesgate.core.pipelineinstance.{PipelineGraph, PipelineStepMessageWrapper}
import net.degols.libs.filesgate.utils.{PipelineInstanceMetadata, PipelineMetadata, Step}

/**
  * Extend this class if you want a specific message distributor.
  *
  * Important note: this class can be called in parallel, so you must be careful about having it thread-safe.
  */
abstract class MessageDistributor {
  protected var _pipelineGraph: PipelineGraph = _
  def setPipelineGraph(pipelineGraph: PipelineGraph): Unit = _pipelineGraph = pipelineGraph

  /**
    *
    * @param m
    * @param targetedStep
    * @param stepStatuses step statuses allowed for the targeted step
    * @return
    */
  def bestPipelineStepStatus(m: PipelineStepMessageWrapper, targetedStep: Step, stepStatuses: List[PipelineStepStatus]): Option[PipelineStepStatus]
}
