package net.degols.libs.filesgate.pipeline.failurehandling

import net.degols.libs.cluster.messages.{BasicLoadBalancerType, ClusterInstance, Communication}
import net.degols.libs.filesgate.core.EngineLeader
import net.degols.libs.filesgate.orm.FileMetadata
import net.degols.libs.filesgate.pipeline.metadata.Metadata.TYPE
import net.degols.libs.filesgate.pipeline.{AbortStep, PipelineStep, PipelineStepMessage, PipelineStepService}
import net.degols.libs.filesgate.pipeline.metadata.MetadataMessage
import net.degols.libs.filesgate.storage.StorageMetadataApi
import net.degols.libs.filesgate.utils.Step
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.{ExecutionContext, Future}

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


class FailureHandling(implicit val ec: ExecutionContext, dbService: StorageMetadataApi) extends FailureHandlingApi {
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  override def process(failureHandlingMessage: FailureHandlingMessage): Future[FailureHandlingMessage] = {
    dbService.upsert(failureHandlingMessage).map(res => {
      failureHandlingMessage
    })
  }
}

object FailureHandling extends PipelineStep{
  override val TYPE: String = "failurehandling"
  override val MANDATORY: Boolean = true
  override val DEFAULT_STEP_NAME: String = "Core.FailureHandling"
  override val defaultStep: Option[Step] = {
    val fullStepName = Communication.fullActorName(EngineLeader.COMPONENT, EngineLeader.PACKAGE, DEFAULT_STEP_NAME)
    Option(Step(TYPE, fullStepName, BasicLoadBalancerType(1, ClusterInstance)))
  }
}