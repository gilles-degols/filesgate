package net.degols.filesgate.libs.filesgate.pipeline.poststorage

import net.degols.filesgate.libs.filesgate.orm.{FileMetadata, RawFileContent}
import net.degols.filesgate.libs.filesgate.pipeline.prestorage.PreStorageMessage
import net.degols.filesgate.libs.filesgate.pipeline.storage.StorageMessage
import net.degols.filesgate.libs.filesgate.pipeline.{AbortStep, PipelineStep, PipelineStepMessage, PipelineStepService}
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json.JsObject

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
  def process(postStorageMessage: PostStorageMessage): PostStorageMessage

  final override def process(message: Any): Any = process(message.asInstanceOf[PostStorageMessage])
}


class PostStorage extends PostStorageApi {
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  override def process(postStorageMessage: PostStorageMessage): PostStorageMessage = {
    logger.debug(s"$id: processing $postStorageMessage")
    postStorageMessage
  }
}

object PostStorage extends PipelineStep{
  override val TYPE: String = "poststorage"
}