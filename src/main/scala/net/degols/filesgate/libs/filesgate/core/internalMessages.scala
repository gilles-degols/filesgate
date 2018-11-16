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
  * Message from the Engine to a PipelineManagerActor saying on which PipelineManager he should work on.
  * @param id
  */
case class PipelineManagerToHandle(id: String) extends EngineInternalMessage

/**
  * Reply by a PipelineManager to the EngineActor saying on which id it is working (based on the PipelineManagerToHandle
  * received). It's acting as acknowledgement
  * @param id
  */
case class PipelineManagerWorkingOn(id: String) extends EngineInternalMessage

/**
  * Information sent by a PipelineManager to the EngineActor to give some status about itself
  */
trait PipelineManagerState
case object PipelineManagerUnreachable extends PipelineManagerState
case object PipelineManagerWaiting extends PipelineManagerState
case object PipelineManagerRunning extends PipelineManagerState

case class PipelineManagerStatus(id: String, var state: PipelineManagerState) extends EngineInternalMessage {
  private var _actorRef: Option[ActorRef] = None
  def setActorRef(actorRef: ActorRef): Unit = _actorRef = Option(actorRef)
  def removeActorRef(): Unit = _actorRef = None
  def actorRef: Option[ActorRef] = actorRef

  def isUnreachable: Boolean = state == PipelineManagerUnreachable
}

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
