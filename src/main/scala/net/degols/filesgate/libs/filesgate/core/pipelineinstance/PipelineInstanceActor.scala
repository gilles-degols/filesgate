package net.degols.filesgate.libs.filesgate.core.pipelineinstance

import akka.actor.{Actor, ActorRef, Kill, Terminated}
import net.degols.filesgate.libs.filesgate.core._
import net.degols.filesgate.libs.filesgate.core.pipelinemanager.{CheckPipelineInstanceState, PipelineManager}
import net.degols.filesgate.libs.filesgate.utils.FilesgateConfiguration
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContext.Implicits.global

case object CheckPipelineStepState

/**
  * Handle one instance of a given pipeline
  */
class PipelineInstanceActor(filesgateConfiguration: FilesgateConfiguration) extends Actor{
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  var pipelineManagerActor: Option[ActorRef] = None

  /**
    * Service to handle everything related to a pipeline.
    */
  val pipelineGraph: PipelineGraph = new PipelineGraph(filesgateConfiguration)
  val pipelineInstance: PipelineInstance = new PipelineInstance(filesgateConfiguration, pipelineGraph)
  pipelineInstance.context = context
  pipelineInstance.pipelineGraph.context = context

  override def receive: Receive = {
    case x: PipelineInstanceToHandle => // This message is necessary to switch to the running state. It is sent by a PipelineManager
      logger.debug("Received the pipeline id on which we should work on.")
      pipelineInstance.setId(x.id)
      pipelineInstance.setPipelineManagerId(x.pipelineManagerId)
      sender() ! PipelineInstanceWorkingOn(x.id, x.pipelineManagerId)

      val frequency = filesgateConfiguration.checkPipelineStepState
      context.system.scheduler.schedule(frequency, frequency, self, CheckPipelineStepState)

      // We watch the PipelineManagerActor, if it dies, we should die to
      pipelineManagerActor = Option(sender())
      context.watch(sender())

      context.become(running)

    case x =>
      logger.error(s"Received unknown message in a PipelineInstanceActor: $x")
  }

  def running: Receive = {
    case x: PipelineInstanceToHandle =>
      // Every PipelineManagerActor will want that we work for them until they now that we are already working for other
      // pipeline. In this case, we do not change for whom we are working for, but we notify them of our current job.
      sender() ! PipelineInstanceWorkingOn(pipelineInstance.id.get, pipelineInstance.pipelineManagerId.get)

    case x: PipelineStepWorkingOn =>
      // A PipelineStep can decide to not work for a given PipelineManager for multiple reasons (too much work already, etc.)
      logger.debug(s"A PipelineStep indicated that it is working on ${x.pipelineManagerId}")
      pipelineInstance.ackFromPipelineStep(sender(), x)

    case Terminated(actorRef) =>
      if(actorRef == pipelineManagerActor.get) {
        logger.error("The PipelineManagerActor just died. We will commit suicide as well.")
        // No need to contact the PipelineStep, they will simply remove this PipelineInstance from their list of "managers"
        // and wait for new work from other instances
        self ! Kill
      } else {
        logger.debug(s"Got a Terminated($actorRef) from a PipelineStepActor.")
        pipelineInstance.diedActorRef(actorRef)
      }

    case CheckPipelineStepState =>
      logger.debug("Received the order to CheckPipelineStepState, verify if we have enough workers to work on our pipeline instance.")
      pipelineInstance.checkEveryPipelineStepStatus()

    case x =>
      logger.error(s"Received unknown message in a PipelineInstanceActor: $x")
  }
}

object PipelineInstanceActor {
  val NAME: String = "Core.PipelineInstanceActor"
}
