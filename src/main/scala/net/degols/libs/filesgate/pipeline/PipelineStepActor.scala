package net.degols.libs.filesgate.pipeline

import akka.NotUsed
import akka.actor.ActorRef
import akka.stream.scaladsl.Source
import net.degols.libs.filesgate.core._
import net.degols.libs.filesgate.orm.FileMetadata
import net.degols.libs.filesgate.pipeline.datasource.{DataSourceApi, DataSourceSeed}
import net.degols.libs.filesgate.utils.PriorityStashedActor
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.{ExecutionContext, Future}

/**
  * Wrapper receiving every message from the core/EngineActor
  */
class PipelineStepActor(implicit val ec: ExecutionContext, pipelineStepService: PipelineStepService) extends PriorityStashedActor {
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  var pipelineInstanceActor: Option[ActorRef] = None

  def running: Receive = {
    case x: PipelineStepToHandle =>
      // Every PipelineInstance will want that we work for them, but
      sender() ! PipelineStepWorkingOn(pipelineStepService.id.get, pipelineStepService.pipelineInstanceId.get, pipelineStepService.pipelineManagerId.get, pipelineStepService.name.get)
      endProcessing(x)

    case message: DataSourceSeed => // Return the Source directly, not a message
      pipelineStepService match {
        case sourceApi: DataSourceApi =>
          val iter: Iterator[FileMetadata] = sourceApi.process(message)
          val source: Source[FileMetadata, NotUsed] = Source.fromIterator(() => iter)
          sender() ! source
        case _ =>
          logger.warn(s"$id: Received a SourceSeed even though we do not have a SourceApi...: ${message}")
      }
      endProcessing(message)

    case message: PipelineStepMessage =>
      logger.debug(s"$id: Start processing PipelineStepMessage $message")
      val res = pipelineStepService.process(message)
      val originalSender = sender()
      res match {
        case future: Future[Any] =>
          val notify = future.map(result => {
            originalSender ! result
            logger.debug(s"$id: End processing PipelineStepMessage $message")})
          endProcessing(message, notify)
        case _ =>
          originalSender ! res
          logger.debug(s"$id: End processing PipelineStepMessage $message")
          endProcessing(message)

      }

    case message =>
      logger.warn(s"$id: Received unknown message in the PipelineStepActor (state: running) ${message}")
      endProcessing(message)
  }

  override def receive = {
    case x: PipelineStepToHandle => // This message is necessary to switch to the running state. It is sent by a PipelineInstance
      logger.debug(s"$id: Received the pipeline instance id for which we should work on.")
      pipelineStepService.setId(x.id)
      pipelineStepService.setPipelineInstanceId(x.pipelineInstanceId)
      pipelineStepService.setPipelineManagerId(x.pipelineManagerId)
      pipelineStepService.setName(x.name)

      sender() ! PipelineStepWorkingOn(x.id, x.pipelineInstanceId, x.pipelineManagerId, x.name)

      // We watch the PipelineInstanceActor, if it dies, we should die too
      pipelineInstanceActor = Option(sender())
      context.watch(sender())

      context.become(running)
      setId(pipelineStepService.id.get+"/"+pipelineStepService.name.get)
      endProcessing(x)


    case message =>
      logger.warn(s"$id: Received unknown message in the PipelineStepActor (state: receive): ${message}")
      endProcessing(message)
  }
}
