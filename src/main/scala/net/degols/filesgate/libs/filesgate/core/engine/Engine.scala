package net.degols.filesgate.libs.filesgate.core.engine

import javax.inject.{Inject, Singleton}

import akka.actor.{ActorContext, ActorRef}
import net.degols.filesgate.libs.cluster.messages.Communication
import net.degols.filesgate.libs.filesgate.core.pipelinemanager.{PipelineManager, PipelineManagerActor}
import net.degols.filesgate.libs.filesgate.core._
import net.degols.filesgate.libs.filesgate.utils.FilesgateConfiguration
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}


@Singleton
class Engine @Inject()(filesgateConfiguration: FilesgateConfiguration) {
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  // Set by the EngineActor when it is started
  var context: ActorContext = _

  /**
    * Contain the status of every PipelineManager (are they running or not)
    * Must remain a lazy val to get the context.
    */
  lazy val pipelineManagers: List[PipelineManagerStatus] = filesgateConfiguration.pipelines.map(metadata => {
    PipelineManagerStatus(id = metadata.id, PipelineManagerUnreachable)
  })

  /**
    * Check status of every PipelineManager. If we still don't have any information from them we try to contact them
    * to give them their id. If we still didn't get an answer since the last attempt, we send a new message.
    */
  def checkEveryPipelineStatus(): Unit = {
    pipelineManagers.filter(_.isUnreachable).foreach(managerStatus => {
      logger.debug(s"Try to contact the PipelineManager in charge of '${managerStatus.id}'")

      val destActorRef = if(managerStatus.actorRef.isDefined) managerStatus.actorRef
      else freePipelineManagerActors().headOption

      destActorRef match {
        case Some(act) =>
          Try {
            val msg = PipelineManagerToHandle(managerStatus.id)
            Communication.sendWithoutReply(context.self, act, msg)
            // We need to remember to which actor we sent the message, to be sure to not assign the work to anybody else
            managerStatus.setActorRef(act)
            context.watch(act)
          } match {
            case Success(res) => // Nothing to do
            case Failure(err) =>
              logger.warn(s"Impossible to send the work assignment to the PipelineManager ${managerStatus.id}")
              managerStatus.removeActorRef()
              context.unwatch(act)
          }
        case None => logger.warn("No PipelineManagerActor available, cannot assign the work to anybody.")
      }
    })
  }

  /**
    * When a PipelineManager received its work order, it sent back an acknowledgement. We use it to update the status locally
    * TODO: We should handle concurrency problems if we receive a very old acknowledgement from a PipelineManager.
    */
  def ackFromPipelineManager(sender: ActorRef, message: PipelineManagerWorkingOn): Unit = {
    val pipelineManager = pipelineManagers.find(pipelineManager => pipelineManager.actorRef.isDefined && pipelineManager.actorRef.get == sender)

    pipelineManager match {
      case Some(status) =>
        if(status.id != message.id) {
          logger.error("We received an ack for an actor ref already assigned to another PipelineManager. This should never happen, system behavior is unknown in this case.")
        } else {
          status.state = PipelineManagerWaiting
          status.setActorRef(sender)
        }
      case None => logger.error(s"Got an acknowledgement from $sender for an actor ref not linked to a known PipelineManager...")
    }
  }

  /**
    * When a PipelineManager has died we are notified, in that case we need to update its status and remove the actoref
    */
  def diedActorRef(actorRef: ActorRef): Unit = {
    val pipelineManager = pipelineManagers.find(pipelineManager => pipelineManager.actorRef.isDefined && pipelineManager.actorRef.get == actorRef)

    pipelineManager match {
      case Some(status) =>
        status.state = PipelineManagerUnreachable
        status.removeActorRef()
      case None => logger.error(s"Got a Terminated($actorRef) for an actor ref not linked to a known PipelineManager...")
    }
  }

  /**
    * Find PipelineManagerActors having not yet any id based on the known actorref locally
    */
  def freePipelineManagerActors(): List[ActorRef] = {
    val knownActorRefs: Map[ActorRef, Boolean] = pipelineManagers.filter(_.actorRef.isDefined).map(_.actorRef.get -> true).toMap
    val fullName = Communication.fullActorName(EngineLeader.COMPONENT, EngineLeader.PACKAGE, PipelineManagerActor.name)
    Communication.actorRefsForId(fullName).filterNot(knownActorRefs.contains)
  }
}
