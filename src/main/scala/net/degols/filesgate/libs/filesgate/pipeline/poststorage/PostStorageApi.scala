package net.degols.filesgate.libs.filesgate.pipeline.poststorage

import net.degols.filesgate.libs.filesgate.orm.{FileMetadata, RawFileContent}
import net.degols.filesgate.libs.filesgate.pipeline.PipelineStepService
import org.slf4j.{Logger, LoggerFactory}

/**
  * @param reason the reason why we aborted the post-storage scripts
  */
case class AbortPostStorage(reason: String)

/**
  * Message sent through every PostStorageApi
  * @param fileMetadata
  * @param rawFileContent
  * @param abortPostStorage if this value is received, we do not go any next post-storage stage
  */
case class PostStorageMessage(fileMetadata: FileMetadata, rawFileContent: RawFileContent, abortPostStorage: Option[AbortPostStorage])

/**
  * Every post-download process must extends this trait.
  */
trait PostStorageApi extends PipelineStepService {
  /**
    * @param postStorageMessage
    * @return
    */
  def process(postStorageMessage: PostStorageMessage): PostStorageMessage

  override def process(message: Any): Any = process(message.asInstanceOf[PostStorageMessage])
}


class PostStorage extends PostStorageApi {
  override def id = "default.postStorage"
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  override def process(postStorageMessage: PostStorageMessage): PostStorageMessage = {
    logger.debug(s"$id: processing $postStorageMessage")
    postStorageMessage
  }
}