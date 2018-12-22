package net.degols.libs.filesgate.pipeline.prestorage

import net.degols.libs.filesgate.orm.{FileMetadata, RawFileContent}
import net.degols.libs.filesgate.pipeline.download.DownloadMessage
import net.degols.libs.filesgate.pipeline.predownload.PreDownloadMessage
import net.degols.libs.filesgate.pipeline.{AbortStep, PipelineStep, PipelineStepMessage, PipelineStepService}
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json.JsObject

/**
  * Message sent through every PreStorageApi
  * @param fileMetadata
  * @param rawFileContent
  * @param abortStorage if this value is received, we do not go any next pre-storage stage
  */
@SerialVersionUID(0L)
case class PreStorageMessage(override val fileMetadata: FileMetadata, override val abort: Option[AbortStep], rawFileContent: Option[RawFileContent], downloadMetadata: Option[JsObject]) extends PipelineStepMessage(fileMetadata, abort)

object PreStorageMessage {
  def from(downloadMessage: DownloadMessage): PreStorageMessage = PreStorageMessage(downloadMessage.fileMetadata, downloadMessage.abort, downloadMessage.rawFileContent, downloadMessage.downloadMetadata)
}

/**
  * Every post-download process must extends this trait.
  */
trait PreStorageApi extends PipelineStepService {
  /**
    * @param preStorageMessage
    * @return
    */
  def process(preStorageMessage: PreStorageMessage): PreStorageMessage

  final override def process(message: Any): Any = process(message.asInstanceOf[PreStorageMessage])
}

class PreStorage extends PreStorageApi {
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  override def process(preStorageMessage: PreStorageMessage): PreStorageMessage = {
    logger.debug(s"$id: processing $preStorageMessage")
    preStorageMessage
  }
}

object PreStorage extends PipelineStep {
  override val TYPE: String = "prestorage"
}