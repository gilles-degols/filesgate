package net.degols.libs.filesgate.pipeline.metadata

import net.degols.libs.filesgate.orm.FileMetadata
import net.degols.libs.filesgate.pipeline.premetadata.PreMetadataMessage
import net.degols.libs.filesgate.pipeline.{AbortStep, PipelineStep, PipelineStepMessage, PipelineStepService}
import net.degols.libs.filesgate.pipeline.storage.StorageMessage
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
case class MetadataMessage(override val fileMetadata: FileMetadata, override val abort: Option[AbortStep], downloadMetadata: Option[JsObject]) extends PipelineStepMessage(fileMetadata, abort)

object MetadataMessage {
  def from(preMetadataMessage: PreMetadataMessage): MetadataMessage = MetadataMessage(preMetadataMessage.fileMetadata, preMetadataMessage.abort, preMetadataMessage.downloadMetadata)
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

class Metadata(implicit val ec: ExecutionContext) extends MetadataApi {
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  override def process(metadataMessage: MetadataMessage): Future[MetadataMessage] = {
    Future {
      logger.debug(s"$id: processing $metadataMessage")
      metadataMessage
    }
  }
}

object Metadata extends PipelineStep {
  override val TYPE: String = "metadata"
}

