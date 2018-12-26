package net.degols.libs.filesgate.pipeline.premetadata

import net.degols.libs.filesgate.orm.{FileMetadata, RawFileContent}
import net.degols.libs.filesgate.pipeline.{AbortStep, PipelineStep, PipelineStepMessage, PipelineStepService}
import net.degols.libs.filesgate.pipeline.download.DownloadMessage
import net.degols.libs.filesgate.pipeline.storage.StorageMessage
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json.JsObject

import scala.concurrent.{ExecutionContext, Future}

/**
  * Message sent through every PreMetadataApi
  *
  * @param fileMetadata
  * @param abortStorage if this value is received, we do not go any next pre-storage stage
  */
@SerialVersionUID(0L)
case class PreMetadataMessage(override val fileMetadata: FileMetadata, override val abort: Option[AbortStep], downloadMetadata: Option[JsObject]) extends PipelineStepMessage(fileMetadata, abort)

object PreMetadataMessage {
  def from(storageMessage: StorageMessage): PreMetadataMessage = PreMetadataMessage(storageMessage.fileMetadata, storageMessage.abort, storageMessage.downloadMetadata)
}

/**
  * Every pre-metadata process must extends this trait.
  */
trait PreMetadataApi extends PipelineStepService {
  /**
    * @param preStorageMessage
    * @return
    */
  def process(preMetadataMessage: PreMetadataMessage): Future[PreMetadataMessage]

  final override def process(message: Any): Any = process(message.asInstanceOf[PreMetadataMessage])
}

class PreMetadata(implicit val ec: ExecutionContext) extends PreMetadataApi {
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  override def process(preMetadataMessage: PreMetadataMessage): Future[PreMetadataMessage] = {
    Future {
      logger.debug(s"$id: processing $preMetadataMessage")
      preMetadataMessage
    }
  }
}

object PreMetadata extends PipelineStep {
  override val TYPE: String = "premetadata"
}
