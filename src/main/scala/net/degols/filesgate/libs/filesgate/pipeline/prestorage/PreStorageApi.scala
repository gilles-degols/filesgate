package net.degols.filesgate.libs.filesgate.pipeline.prestorage

import net.degols.filesgate.libs.filesgate.orm.{FileMetadata, RawFileContent}
import net.degols.filesgate.libs.filesgate.pipeline.PipelineStepService
import org.slf4j.{Logger, LoggerFactory}
/**
  * @param reason the reason why we aborted the storage
  */
@SerialVersionUID(0L)
case class AbortStorage(reason: String)

/**
  * Message sent through every PreStorageApi
  * @param fileMetadata
  * @param rawFileContent
  * @param abortStorage if this value is received, we do not go any next pre-storage stage
  */
@SerialVersionUID(0L)
case class PreStorageMessage(fileMetadata: FileMetadata, rawFileContent: RawFileContent, abortStorage: Option[AbortStorage])

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

object PreStorage {
  val TYPE: String = "prestorage"
}