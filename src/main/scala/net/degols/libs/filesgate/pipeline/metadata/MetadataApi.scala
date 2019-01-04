package net.degols.libs.filesgate.pipeline.metadata

import net.degols.libs.filesgate.orm.FileMetadata
import net.degols.libs.filesgate.pipeline.premetadata.PreMetadataMessage
import net.degols.libs.filesgate.pipeline.{AbortStep, PipelineStep, PipelineStepMessage, PipelineStepService}
import net.degols.libs.filesgate.storage.StorageMetadataApi
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json.JsObject

import scala.concurrent.{ExecutionContext, Future}

/**
  * Message sent through every MetadataApi
  *
  * @param fileMetadata
  * @param abortStorage if this value is received, we do not go any next pre-storage stage
  */
@SerialVersionUID(0L)
case class MetadataMessage(override val fileMetadata: FileMetadata, override val abort: Option[AbortStep]) extends PipelineStepMessage(fileMetadata, abort)

object MetadataMessage {
  def from(preMetadataMessage: PreMetadataMessage): MetadataMessage = MetadataMessage(preMetadataMessage.fileMetadata, preMetadataMessage.abort)
}

/**
  * Every metadata process must extends this trait.
  */
trait MetadataApi extends PipelineStepService {
  /**
    * @param preStorageMessage
    * @return
    */
  def process(metadataMessage: MetadataMessage): Future[MetadataMessage]

  final override def process(message: Any): Any = process(message.asInstanceOf[MetadataMessage])
}

class Metadata(dbService: StorageMetadataApi)(implicit val ec: ExecutionContext) extends MetadataApi {
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  override def process(metadataMessage: MetadataMessage): Future[MetadataMessage] = {
    dbService.upsert(metadataMessage.fileMetadata).map(res => {
      metadataMessage
    })
  }
}

object Metadata extends PipelineStep {
  override val TYPE: String = "metadata"
  override val IMPORTANT_STEP: Boolean = true
  override val DEFAULT_STEP_NAME: String = "Core.Metadata"
}

