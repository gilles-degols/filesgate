package net.degols.libs.filesgate.pipeline

import net.degols.libs.filesgate.utils.Step

/**
  * Contain the logic of any PipelineStep. Must be extended when we implement a PipelineStep
  */
abstract class PipelineStepService {
  // Set by the PipelineStepActor once it has received a message from the EngineActor. Cannot be changed afterwards
  private var _id: Option[String] = None
  private[filesgate] def setId(id: String): Unit = {
    if(_id.isDefined) {
      throw new Exception("The id of a PipelineStep cannot be changed once initialized!")
    } else {
      _id = Option(id)
    }
  }
  def id: Option[String] = _id

  private var _pipelineInstanceId: Option[String] = None
  private[filesgate] def setPipelineInstanceId(pipelineInstanceId: String): Unit = {
    if(_pipelineInstanceId.isDefined) {
      throw new Exception("The pipelineInstanceId of a PipelineStep cannot be changed once initialized!")
    } else {
      _pipelineInstanceId = Option(pipelineInstanceId)
    }
  }
  def pipelineInstanceId: Option[String] = _pipelineInstanceId

  private var _pipelineManagerId: Option[String] = None
  private[filesgate] def setPipelineManagerId(pipelineManagerId: String): Unit = {
    if(_pipelineManagerId.isDefined) {
      throw new Exception("The pipelineManagerId of a PipelineStep cannot be changed once initialized!")
    } else {
      _pipelineManagerId = Option(pipelineManagerId)
    }
  }
  def pipelineManagerId: Option[String] = _pipelineManagerId

  private var _step: Option[Step] = None
  private[filesgate] def setStep(step: Step): Unit = {
    if(_step.isDefined) {
      throw new Exception("The step handle by a PipelineStepService cannot be changed once initialized!")
    } else {
      _step = Option(step)
    }
  }
  def step: Option[Step] = _step


  /**
    * To be implemented by the user
    * @param message
    * @return
    */
  def process(message: Any): Any
}
