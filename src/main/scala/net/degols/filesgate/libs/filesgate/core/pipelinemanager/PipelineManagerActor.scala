package net.degols.filesgate.libs.filesgate.core.pipelinemanager

import akka.actor.{Actor, ActorContext, ActorRef, Kill, Terminated}
import net.degols.filesgate.libs.cluster.core.Cluster
import net.degols.filesgate.libs.filesgate.core.engine.CheckPipelineManagerState
import net.degols.filesgate.libs.filesgate.core.{PipelineManagerToHandle, PipelineManagerWorkingOn, RemotePipelineStep, RemoteStartPipelineStepInstance}
import net.degols.filesgate.libs.filesgate.utils.FilesgateConfiguration
import net.degols.filesgate.service.Tools
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

case object CheckPipelineInstanceState

/**
  * Handle everything related to a specific Pipeline. The related pipeline to handle is given by the EngineActor afterwards.
  * We can have a lot of those PipelineManager started by default as we are not sure how many are needed by the user. They
  * will remain idle. Each PipelineManager is in charge of sending jobs to PipelineInstances.
  * In short:
  *  -> PipelineManager cannot be customized by the developer. Any PipelineManager can be linked to any pipeline defined by the user
  *  -> many PipelineManagers to handle all types of pipeline.
  *  -> one pipelineManager by type of pipeline
  *  -> each PipelineManager, based on the PipelineMetadata, is linked to a specific set of PipelineInstances (to be able to have a specific load balancer for each of them)
  *  -> the PipelineInstances are in charge of downloading the files themselves, in a streaming way. So we can have 1000 actors running at the same time in some cases.
  *  -> Each PipelineInstance will be linked to various actors, to read urls to download, to download them, etc. Sometimes we have 1 actor to read a lot of data, and 10 actors to download, then again 1 actor to write them, all those things linked to one PipelineInstance.
  */
class PipelineManagerActor(filesgateConfiguration: FilesgateConfiguration) extends Actor {
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  var engineActor: Option[ActorRef] = None

  /**
    * Service to handle everything related to a pipeline.
    */
  val pipelineManager: PipelineManager = new PipelineManager(filesgateConfiguration)
  pipelineManager.context = context


  override def receive: Receive = {
    case x: PipelineManagerToHandle => // This message is necessary to switch to the running state
      logger.debug("Received the pipeline id on which we should work on.")
      pipelineManager.setId(x.id)
      sender() ! PipelineManagerWorkingOn(x.id)

      val frequency = filesgateConfiguration.checkPipelineInstanceState
      context.system.scheduler.schedule(frequency, frequency, self, CheckPipelineInstanceState)

      // We watch the EngineActor, if it dies, we should die to
      engineActor = Option(sender())
      context.watch(sender())

      context.become(running)

    case x =>
      logger.error(s"Received unknown message in a PipelineManagerActor: $x")
  }

  def running: Receive = {
    case x: PipelineManagerToHandle =>
      // We could receive a duplicate message if we are unlucky, but this should normally not happen
      if(x.id != pipelineManager.id.get) {
        logger.error(s"We received the order to handle a specific pipeline id (${x.id}), but we already got a different job to do (${pipelineManager.id.get})!")
      } else {
        logger.warn(s"We received the order to handle a specific pipeline id (${x.id}), but we are already working on it. We reply just in case the message was lost.")
        sender() ! PipelineManagerWorkingOn(x.id)
      }

    case Terminated(actorRef) =>
      if(actorRef == engineActor.get) {
        logger.error("The EngineActor just died. We will commit suicide as well.")
        self ! Kill
      } else {
        logger.debug(s"Got a Terminated($actorRef) from a PipelineInstance.")
        // TODO
      }

    case CheckPipelineInstanceState =>
      logger.debug("Received the order to CheckPipelineInstanceState, verify if we have communicated with all of them")
      // TODO: Find a way to have a limit of the number of PipelineInstances based on the related Balancer
      pipelineManager.checkEveryPipelineInstanceStatus(???)

    case x =>
      logger.error(s"Received unknown message in a PipelineManagerActor: $x")
  }
}

object PipelineManagerActor {
  val name: String = "Core.PipelineManagerActor"
}