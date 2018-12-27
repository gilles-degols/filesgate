package net.degols.libs.filesgate.core

import akka.actor.ActorRef
import net.degols.libs.filesgate.utils.{PriorityStashedMessage, Step, UnknownPipelineStep}

/**
  * Every message sent to PipelineStepActor must extend this one. Any priority in [0;10[ is reserved for internal use of
  * PriorityStashedActor. Priority [10;20[ is reserved for filesgate itself.
  * @param priority
  */
@SerialVersionUID(0L)
abstract class EngineInternalMessage(priority: Int = 15) extends PriorityStashedMessage(priority)

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
  * Message from the PipelineManagerActor to a PipelineInstanceActor saying for which PipelineManager he should work on.
  * @param id
  */
case class PipelineInstanceToHandle(id: String, pipelineManagerId: String) extends EngineInternalMessage

/**
  * Reply by a PipelineInstance to the EngineActor saying on which id it is working (based on the PipelineInstanceToHandle
  * received). It's acting as acknowledgement or as notification if the PipelineInstance is already working for another
  * PipelineManager
  * @param id
  */
case class PipelineInstanceWorkingOn(id: String, pipelineManagerId: String) extends EngineInternalMessage


/**
  * Demand from a PipelineInstanceActor to know if it agrees to work for a given PipelineInstanceId
  * The Name parameter is the full name of the actor
  */
case class PipelineStepToHandle(id: String, pipelineInstanceId: String, pipelineManagerId: String, step: Step) extends EngineInternalMessage

/**
  * Reply by a PipelineStep to the EngineActor saying on which id it is working (based on the PipelineInstanceToHandle
  * received). A PipelineStep can only work for one PipelineInstance at a time to simplify the code and the (performance) debugging
  * It's acting as acknowledgement or as a refusal to work on the given task
  * @param id
  */
case class PipelineStepWorkingOn(id: String, pipelineInstanceId: String, pipelineManagerId: String, step: Step) extends EngineInternalMessage

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
  def actorRef: Option[ActorRef] = _actorRef

  def isUnreachable: Boolean = state == PipelineManagerUnreachable
}

trait PipelineInstanceState
case object PipelineInstanceUnreachable extends PipelineInstanceState
case object PipelineInstanceWaiting extends PipelineInstanceState
case object PipelineInstanceRunning extends PipelineInstanceState

case class PipelineInstanceStatus(var pipelineManagerId: Option[String], var state: PipelineInstanceState) extends EngineInternalMessage {
  private var _actorRef: Option[ActorRef] = None
  def setActorRef(actorRef: ActorRef): Unit = _actorRef = Option(actorRef)
  def removeActorRef(): Unit = _actorRef = None
  def actorRef: Option[ActorRef] = _actorRef

  def isUnreachable: Boolean = state == PipelineInstanceUnreachable
}

trait PipelineStepState
case object PipelineStepUnknown extends PipelineStepState
case object PipelineStepUnreachable extends PipelineStepState
case object PipelineStepWaiting extends PipelineStepState
case object PipelineStepRunning extends PipelineStepState

/**
  * @param step
  * @param pipelineInstances the key is the PipelineInstanceId
  */
case class PipelineStepStatus(step: Step, var pipelineInstanceId: String, var pipelineStepState: PipelineStepState = PipelineStepUnknown) extends EngineInternalMessage {
  private var _actorRef: Option[ActorRef] = None
  def setActorRef(actorRef: ActorRef): Unit = _actorRef = Option(actorRef)
  def removeActorRef(): Unit = _actorRef = None
  def actorRef: Option[ActorRef] = _actorRef

  def isUnreachable: Boolean = {
    pipelineStepState == PipelineStepUnreachable || pipelineStepState == PipelineStepUnknown
  }
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
