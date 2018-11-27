package net.degols.filesgate.libs.filesgate.core.engine

import akka.actor.{Actor, Terminated}
import net.degols.filesgate.libs.filesgate.core.{PipelineManagerWorkingOn, Start}
import net.degols.filesgate.libs.filesgate.utils.FilesgateConfiguration
import org.slf4j.LoggerFactory
import scala.concurrent.ExecutionContext.Implicits.global

case object CheckPipelineManagerState

/**
  * Handle all the PipelineManager in the application, and in general, the entire job of filesgate
  */
class EngineActor(engine: Engine, filesgateConfiguration: FilesgateConfiguration) extends Actor{
  private val logger = LoggerFactory.getLogger(getClass)

  override def receive: Receive = {
    case x: Start =>
      context.become(running)
      engine.context = context
      val frequency = filesgateConfiguration.checkPipelineManagerState
      context.system.scheduler.schedule(frequency, frequency, self, CheckPipelineManagerState)

    case x =>
      logger.error(s"[receive state] Received unknown message in the EngineActor: $x")
  }

  def running: Receive = {
    case CheckPipelineManagerState =>
      logger.debug("Check status of every PipelineManager.")
    case x: PipelineManagerWorkingOn =>
      logger.debug(s"Received ack from the PipelineManager ${sender()} working on ${x.id}")
      engine.ackFromPipelineManager(sender(), x)
    case Terminated(actorRef) =>
      logger.warn(s"An watched actor has just died or has been disconnected: $actorRef")
      engine.diedActorRef(actorRef)

    case x =>
      logger.error(s"Received unknown message in the EngineActor: $x")
  }
}

object EngineActor {
  val name: String = "Core.EngineActor"
}