package net.degols.filesgate.libs.filesgate.engine.core

import akka.actor.ActorRef
import net.degols.filesgate.engine.UnknownPipelineStep
import net.degols.filesgate.engine.cluster.{FilesGateInstance, Node, PipelineStep}
import play.api.libs.json.JsResult.Exception


trait EngineInternalMessage

/**
  * Internal message to order an Actor to become master
  */
case class BecomeRunning() extends EngineInternalMessage

/**
  * Internal message to start the actor and wait for the order
  */
case class BecomeWaiting() extends EngineInternalMessage

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
  * Convert remoteMessages to internalMessages
  */

object Node {
  def from(remoteFilesGateInstance: RemoteFilesGateInstance): Node = {
    new Node(remoteFilesGateInstance.hostname)
  }
}

object FilesGateInstance {
  def from(remoteFilesGateInstance: RemoteFilesGateInstance, actorRef: ActorRef): FilesGateInstance = {
    new FilesGateInstance(s"${remoteFilesGateInstance.hostname}/${remoteFilesGateInstance.id}", actorRef)
  }
}

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

object PipelineStep {
  def from(remotePipelineStep: RemotePipelineStep): PipelineStep = {
    new PipelineStep(remotePipelineStep.id, PipelineStepType.from(remotePipelineStep.tpe))
  }
}