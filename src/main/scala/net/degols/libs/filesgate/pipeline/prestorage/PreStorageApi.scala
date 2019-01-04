package net.degols.libs.filesgate.pipeline.prestorage

import net.degols.libs.filesgate.orm.FileMetadata
import net.degols.libs.filesgate.pipeline.download.DownloadMessage
import net.degols.libs.filesgate.pipeline.{AbortStep, PipelineStep, PipelineStepMessage, PipelineStepService}
import net.degols.libs.filesgate.utils.DownloadedFile
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json.JsObject

import scala.concurrent.{ExecutionContext, Future}

/**
  * Message sent through every PreStorageApi
  * @param fileMetadata
  * @param abortStorage if this value is received, we do not go any next pre-storage stage
  */
@SerialVersionUID(0L)
case class PreStorageMessage(override val fileMetadata: FileMetadata, override val abort: Option[AbortStep], downloadedFile: Option[DownloadedFile]) extends PipelineStepMessage(fileMetadata, abort)

object PreStorageMessage {
  def from(downloadMessage: DownloadMessage): PreStorageMessage = PreStorageMessage(downloadMessage.fileMetadata, downloadMessage.abort, downloadMessage.downloadedFile)
}

/**
  * Every post-download process must extends this trait.
  */
trait PreStorageApi extends PipelineStepService {
  /**
    * @param preStorageMessage
    * @return
    */
  def process(preStorageMessage: PreStorageMessage): Future[PreStorageMessage]

  final override def process(message: Any): Any = process(message.asInstanceOf[PreStorageMessage])
}

class PreStorage(implicit val ec: ExecutionContext) extends PreStorageApi {
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  override def process(preStorageMessage: PreStorageMessage): Future[PreStorageMessage] = {
    Future {
      logger.debug(s"$id: processing $preStorageMessage")
      preStorageMessage
    }
  }
}

object PreStorage extends PipelineStep {
  override val TYPE: String = "prestorage"
}