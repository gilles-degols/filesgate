package net.degols.libs.filesgate.pipeline

import akka.NotUsed
import akka.actor.{Actor, ActorRef}
import akka.stream.scaladsl.Source
import net.degols.libs.filesgate.core.pipelineinstance.CheckPipelineStepState
import net.degols.libs.filesgate.core._
import net.degols.libs.filesgate.orm.FileMetadata
import net.degols.libs.filesgate.pipeline.download.{DownloadApi, DownloadMessage}
import net.degols.libs.filesgate.pipeline.poststorage.{PostStorageApi, PostStorageMessage}
import net.degols.libs.filesgate.pipeline.predownload.{PreDownloadApi, PreDownloadMessage}
import net.degols.libs.filesgate.pipeline.prestorage.{PreStorageApi, PreStorageMessage}
import net.degols.libs.filesgate.pipeline.datasource.{DataSourceApi, DataSourceSeed}
import net.degols.libs.filesgate.pipeline.matcher.MatcherApi
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

    case message: PipelineStepMessage =>
      sender() ! pipelineStepService.process(message)

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
