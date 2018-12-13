package net.degols.filesgate.libs.filesgate.pipeline

import akka.NotUsed
import akka.actor.{Actor, ActorRef}
import akka.stream.scaladsl.Source
import net.degols.filesgate.libs.filesgate.core.pipelineinstance.CheckPipelineStepState
import net.degols.filesgate.libs.filesgate.core._
import net.degols.filesgate.libs.filesgate.orm.FileMetadata
import net.degols.filesgate.libs.filesgate.pipeline.download.{DownloadApi, DownloadMessage}
import net.degols.filesgate.libs.filesgate.pipeline.poststorage.{PostStorageApi, PostStorageMessage}
import net.degols.filesgate.libs.filesgate.pipeline.predownload.{PreDownloadApi, PreDownloadMessage}
import net.degols.filesgate.libs.filesgate.pipeline.prestorage.{PreStorageApi, PreStorageMessage}
import net.degols.filesgate.libs.filesgate.pipeline.datasource.{DataSourceApi, DataSourceSeed}
import net.degols.filesgate.libs.filesgate.pipeline.matcher.MatcherApi
import org.slf4j.{Logger, LoggerFactory}

/**
  * Wrapper receiving every message from the core/EngineActor
  */
class PipelineStepActor(pipelineStepService: PipelineStepService) extends Actor {
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  var pipelineInstanceActor: Option[ActorRef] = None

  def running: Receive = {
    case x: PipelineStepToHandle =>
      // Every PipelineInstance will want that we work for them, but
      sender() ! PipelineStepWorkingOn(pipelineStepService.id.get, pipelineStepService.pipelineInstanceId.get, pipelineStepService.pipelineManagerId.get, pipelineStepService.name.get)

    case message: DataSourceSeed => // Return the Source directly, not a message
      pipelineStepService match {
        case sourceApi: DataSourceApi =>
          val iter: Iterator[FileMetadata] = sourceApi.process(message)
          val source: Source[FileMetadata, NotUsed] = Source.fromIterator(() => iter)
          sender() ! source
        case _ =>
          logger.warn(s"Received a SourceSeed even though we do not have a SourceApi...: ${message}")
      }

    case message: FileMetadata =>
      pipelineStepService match {
        case matcherApi: MatcherApi => // Simply return true/false
          sender() ! matcherApi.process(message)
        case _ =>
          logger.warn(s"Received a FileMetadata ($message) even though we do not have an appropriate PipelineStep: $pipelineStepService")
      }

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
    case x: PipelineStepToHandle => // This message is necessary to switch to the running state. It is sent by a PipelineInstance
      logger.debug("Received the pipeline instance id for which we should work on.")
      pipelineStepService.setId(x.id)
      pipelineStepService.setPipelineInstanceId(x.pipelineInstanceId)
      pipelineStepService.setPipelineManagerId(x.pipelineManagerId)
      pipelineStepService.setName(x.name)

      sender() ! PipelineStepWorkingOn(x.id, x.pipelineInstanceId, x.pipelineManagerId, x.name)

      // We watch the PipelineInstanceActor, if it dies, we should die too
      pipelineInstanceActor = Option(sender())
      context.watch(sender())

      context.become(running)

    case message =>
      logger.warn(s"Received unknown message in the PipelineStepActor (state: receive): ${message}")
  }
}
