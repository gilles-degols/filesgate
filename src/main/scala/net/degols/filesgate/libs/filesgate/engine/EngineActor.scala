package net.degols.filesgate.libs.filesgate.engine.core

import javax.inject.{Inject, Singleton}

import akka.actor.Actor
import net.degols.filesgate.engine.cluster.Cluster
import org.slf4j.{Logger, LoggerFactory}

/**
  * In charge of receiving every PipelineStep, monitor them and so on. Only one instance of a EngineActor per cluster instance
  * as this is the general orchestrator of the system (an election system must be made in order to only have one instance
  * of EngineActor in the state "running" through all machines
  * @param engine
  */
@Singleton
class EngineActor @Inject()(
                             engine: Engine,
                             cluster: Cluster) extends Actor {
  private val logger: Logger = LoggerFactory.getLogger(getClass)
  engine.context = context

  override def preStart(): Unit = {
    self ! BecomeWaiting()

    // TODO: We need the external Election library to switch to the "running" context, but for now, we directly start the system
    self ! BecomeRunning()
  }

  override def receive = {
    case message: BecomeWaiting =>
      logger.info("EngineActor has received the order to become master.")
      context.become(waiting)
    case message =>
      logger.warn(s"Received unknown message in the EngineActor (receive state): ${message}")
  }

  def waiting: Receive = {
    case message: BecomeRunning =>
      logger.info("EngineActor has received the order to become running.")
      context.become(running)
    case message =>
      logger.warn(s"Received unknown message in the EngineActor (waiting state): ${message}")
  }

  def running: Receive = {
    case message: BecomeWaiting =>
      logger.info("EngineActor has received the order to become waiting.")
      context.become(waiting)

    case message: RegisterPipelineStep =>
      logger.info("Register a PipelineStep")
      val rawNode = Node.from(message.remoteFilesGateInstance)
      val rawFilesGateInstance = FilesGateInstance.from(message.remoteFilesGateInstance, context.sender())
      val rawPipelineStep = PipelineStep.from(message.remotePipelineStep)
      cluster.registerPipelineStep(rawNode, rawFilesGateInstance, rawPipelineStep)
      self ! CheckPipelineStatus()

    case message: CheckPipelineStatus =>
      logger.info("Check the pipeline status: do we have all steps to start a pipeline / stop it?")
      engine.checkEveryPipelineStatus()

    case message: StartPipelineInstances =>
      logger.info("Start the Pipeline: ask for the creation of various PipelineSteps instances.")
      engine.pipelineManagers.find(_.id == message.pipelineId) match {
        case Some(res) =>
          res.startPipelineInstances()
          res.setRunning(true)
        case None => // Should never happen
      }

    case message: StopPipelineInstances =>
      logger.info("Stop the Pipeline: ask to PipelineSteps to stop processing.")
      engine.pipelineManagers.find(_.id == message.pipelineId) match {
        case Some(res) =>
          res.stopPipelineInstances()
          res.setRunning(false)
        case None => // Should never happen
      }

    case message =>
      logger.warn(s"Received unknown message in the EngineActor (running state): ${message}")
  }
}
