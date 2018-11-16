package net.degols.filesgate.libs.filesgate.core

import akka.stream.StreamRefMessages.ActorRef
import net.degols.filesgate.engine.UnknownPipelineStep
import net.degols.filesgate.engine.cluster.PipelineStep

/**
  * Every message in this file must be able to go through AkkaRemote, so it must be serializable. To avoid problems, only
  * put the minimum number of information in it.
  */

trait EngineRemoteMessage

/**
  * Represent a specific FilesGate JVM
  * @param hostname can also be an ip address
  * @param id we could have multiple instances of FilesGate running on the same machine. This id is a way to tell them apart (for example we could use the pid)
  *           this information will be merged with the hostname to have a unique identifier.
  */
case class RemoteFilesGateInstance(hostname: String, id: String) {
  def canEqual(a: Any) = a.isInstanceOf[RemoteFilesGateInstance]
  override def equals(that: Any): Boolean =
    that match {
      case that: RemoteFilesGateInstance => that.canEqual(this) && this.hashCode == that.hashCode
      case _ => false
    }
  override def hashCode: Int = hostname.hashCode
}

abstract class RemotePipelineStepType(val id: String)
case class RemotePreDownloadStep() extends RemotePipelineStepType(id = "PreDownloadStep")
case class RemotePreStorageStep() extends RemotePipelineStepType(id = "PreStorageStep")
case class RemotePostStorageStep() extends RemotePipelineStepType(id = "PostStorageStep")

/**
  * General information about a PipelineStep
  * @param id
  * @param tpe
  */
case class RemotePipelineStep(id: String, tpe: RemotePipelineStepType) extends EngineRemoteMessage

/**
  * Message used to register a pipeline step. This does not correspond an instance of an ActorRef of the PipelineStep, but rather, the existence
  * of a specific class in a given jvm (normally 1 jvm/node).
  */
case class RegisterPipelineStep(remotePipelineStep: RemotePipelineStep, remoteFilesGateInstance: RemoteFilesGateInstance) extends EngineRemoteMessage


/**
  * Ask to start a PipelineStepInstance
  */
case class RemoteStartPipelineStepInstance(remotePipelineStep: RemotePipelineStep)


object RemotePipelineStepType {
  def from(pipelineStepType: PipelineStepType): RemotePipelineStepType = {
    pipelineStepType.id match {
      case "PreDownloadStep" => RemotePreDownloadStep()
      case "PreStorageStep" => RemotePreStorageStep()
      case "PostStorageStep" => RemotePostStorageStep()
      case x => throw new UnknownPipelineStep(x)
    }
  }
}

object RemotePipelineStep {
  def from(pipelineStep: PipelineStep): RemotePipelineStep = {
    new RemotePipelineStep(pipelineStep.id, RemotePipelineStepType.from(pipelineStep.tpe))
  }
}