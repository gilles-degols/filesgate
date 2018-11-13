package net.degols.filesgate.libs.filesgate.pipeline

import akka.actor.Actor
import net.degols.filesgate.engine.core.{BecomeRunning, RemoteStartPipelineStepInstance}
import net.degols.filesgate.libs.filesgate.pipeline.download.{DownloadApi, DownloadMessage}
import net.degols.filesgate.libs.filesgate.pipeline.poststorage.{PostStorageApi, PostStorageMessage}
import net.degols.filesgate.libs.filesgate.pipeline.predownload.{PreDownloadApi, PreDownloadMessage}
import net.degols.filesgate.libs.filesgate.pipeline.prestorage.{PreStorageApi, PreStorageMessage}
import org.slf4j.{Logger, LoggerFactory}

/**
  * Wrapper receiving every message from the core/EngineActor
  */
class PipelineStepActor(pipelineStepService: PipelineStepService) extends Actor {
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  override def preStart(): Unit = {
    self ! BecomeRunning()
  }

  def running: Receive = {
    case message: PreDownloadMessage =>
      if(pipelineStepService.isInstanceOf[PreDownloadApi]) {
        pipelineStepService.process(pipelineStepService)
      } else {
        logger.warn(s"Received a PreDownloadMessage even though we do not have a PreDownloadApi...: ${message}")
      }

    case message: DownloadMessage =>
      if(pipelineStepService.isInstanceOf[DownloadApi]) {
        pipelineStepService.process(pipelineStepService)
      } else {
        logger.warn(s"Received a DownloadMessage even though we do not have a DownloadApi...: ${message}")
      }

    case message: PreStorageMessage =>
      if(pipelineStepService.isInstanceOf[PreStorageApi]) {
        pipelineStepService.process(pipelineStepService)
      } else {
        logger.warn(s"Received a PreStorageMessage even though we do not have a PreStorageApi...: ${message}")
      }

    case message: PostStorageMessage =>
      if(pipelineStepService.isInstanceOf[PostStorageApi]) {
        pipelineStepService.process(pipelineStepService)
      } else {
        logger.warn(s"Received a PostStorageMessage even though we do not have a PostStorageApi...: ${message}")
      }

    case message =>
      logger.warn(s"Received unknown message in the PipelineStepActor (state: running) ${message}")
  }

  override def receive = {
    case message: RemoteStartPipelineStepInstance =>
      logger.debug("Received the order to start the current PipelineStepInstance.")
      context.become(running)

    case message =>
      logger.warn(s"Received unknown message in the PipelineStepActor (state: receive): ${message}")
  }
}
