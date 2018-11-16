package net.degols.filesgate.libs.filesgate.core

import akka.actor.ActorRef
import net.degols.filesgate.engine.UnknownPipelineStep
import net.degols.filesgate.engine.cluster.{FilesGateInstance, Node, PipelineStep}
import play.api.libs.json.JsResult.Exception


trait EngineInternalMessage

abstract class PipelineStepType(val id: String)
case class PreDownloadStep() extends PipelineStepType(id = "PreDownloadStep")
case class PreStorageStep() extends PipelineStepType(id = "PreStorageStep")
case class PostStorageStep() extends PipelineStepType(id = "PostStorageStep")

/**
  * Check for the general cluster status.
  */
case class CheckPipelineStatus() extends EngineInternalMessage

case class StartPipelineInstances(pipelineId: String) extends EngineInternalMessage
case class StopPipelineInstances(pipelineId: String) extends EngineInternalMessage

/**
  * Internal messages
  */
case class Start() extends EngineInternalMessage

/**
  * Convert remoteMessages to internalMessages
  */
object PipelineStepType {
  def from(remotePipelineStepType: RemotePipelineStepType): PipelineStepType = {
    remotePipelineStepType.id match {
      case "PreDownloadStep" => PreDownloadStep()
      case "PreStorageStep" => PreStorageStep()
      case "PostStorageStep" => PostStorageStep()
      case x => throw new UnknownPipelineStep(x)
    }
  }
}
