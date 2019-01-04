package net.degols.libs.filesgate.pipeline.failurehandling

import net.degols.libs.filesgate.pipeline.{PipelineStep, PipelineStepMessage, PipelineStepService}
import net.degols.libs.filesgate.storage.StorageMetadataApi
import net.degols.libs.cluster.{Tools => ClusterTools}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/**
  * Message wrapping any other PipelineStepMessage with abort=true and or exception
  *
  */
@SerialVersionUID(0L)
case class FailureHandlingMessage(initialMessage: PipelineStepMessage, outputMessage: Option[PipelineStepMessage], exception: Option[Throwable]) extends PipelineStepMessage(initialMessage.fileMetadata, initialMessage.abort)

object FailureHandlingMessage {
  def from(initialMessage: PipelineStepMessage, outputMessage: Option[PipelineStepMessage], exception: Option[Throwable]): FailureHandlingMessage = FailureHandlingMessage(initialMessage, outputMessage, exception)
}

/**
  * Every failure handling process must extends this trait.
  */
trait FailureHandlingApi extends PipelineStepService {
  def process(failureHandlingMessage: FailureHandlingMessage): Future[FailureHandlingMessage]

  final override def process(message: Any): Any = process(message.asInstanceOf[FailureHandlingMessage])
}


class FailureHandling(dbService: StorageMetadataApi)(implicit val ec: ExecutionContext) extends FailureHandlingApi {
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  override def process(failureHandlingMessage: FailureHandlingMessage): Future[FailureHandlingMessage] = {
    dbService.upsert(failureHandlingMessage).transformWith{
      case Success(value) => Future{failureHandlingMessage}
      case Failure(err) => Future{
        logger.error(s"Problem while processing the failure handling: ${ClusterTools.formatStacktrace(err)}")
        throw err
      }
    }
  }
}

object FailureHandling extends PipelineStep{
  override val TYPE: String = "failurehandling"
  override val IMPORTANT_STEP: Boolean = true
  override val DEFAULT_STEP_NAME: String = "Core.FailureHandling"
}