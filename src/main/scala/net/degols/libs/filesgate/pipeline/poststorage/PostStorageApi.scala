package net.degols.libs.filesgate.pipeline.poststorage

import net.degols.libs.filesgate.orm.{FileMetadata, RawFileContent}
import net.degols.libs.filesgate.pipeline.prestorage.PreStorageMessage
import net.degols.libs.filesgate.pipeline.storage.StorageMessage
import net.degols.libs.filesgate.pipeline.{AbortStep, PipelineStep, PipelineStepMessage, PipelineStepService}
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json.JsObject

import scala.concurrent.{ExecutionContext, Future}

/**
  * Message sent through every PostStorageApi
  * @param fileMetadata
  * @param rawFileContent
  * @param abortPostStorage if this value is received, we do not go any next post-storage stage
  */
@SerialVersionUID(0L)
case class PostStorageMessage(override val fileMetadata: FileMetadata, override val abort: Option[AbortStep], rawFileContent: Option[RawFileContent], downloadMetadata: Option[JsObject]) extends PipelineStepMessage(fileMetadata, abort)

object PostStorageMessage {
  def from(storageMessage: StorageMessage): PostStorageMessage = PostStorageMessage(storageMessage.fileMetadata, storageMessage.abort, storageMessage.rawFileContent, storageMessage.downloadMetadata)
}

/**
  * Every post-download process must extends this trait.
  */
trait PostStorageApi extends PipelineStepService {
  /**
    * @param postStorageMessage
    * @return
    */
  def process(postStorageMessage: PostStorageMessage): Future[PostStorageMessage]

  final override def process(message: Any): Any = process(message.asInstanceOf[PostStorageMessage])
}


class PostStorage(implicit val ec: ExecutionContext) extends PostStorageApi {
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  override def process(postStorageMessage: PostStorageMessage): Future[PostStorageMessage] = {
    Future{
      logger.debug(s"$id: processing $postStorageMessage")
      postStorageMessage
    }
  }
}

object PostStorage extends PipelineStep{
  override val TYPE: String = "poststorage"
}