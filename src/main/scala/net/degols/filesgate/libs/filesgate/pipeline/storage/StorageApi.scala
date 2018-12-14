package net.degols.filesgate.libs.filesgate.pipeline.storage

import net.degols.filesgate.libs.filesgate.orm.{FileMetadata, RawFileContent}
import net.degols.filesgate.libs.filesgate.pipeline.download.DownloadMessage
import net.degols.filesgate.libs.filesgate.pipeline.prestorage.PreStorageMessage
import net.degols.filesgate.libs.filesgate.pipeline.{AbortStep, PipelineStep, PipelineStepMessage, PipelineStepService}
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json.JsObject

import scala.util.Try

@SerialVersionUID(0L)
case class StorageMessage(override val fileMetadata: FileMetadata, override val abort: Option[AbortStep], rawFileContent: Option[RawFileContent], downloadMetadata: Option[JsObject]) extends PipelineStepMessage(fileMetadata, abort)

object StorageMessage {
  def from(preStorageMessage: PreStorageMessage): StorageMessage = StorageMessage(preStorageMessage.fileMetadata, preStorageMessage.abort, preStorageMessage.rawFileContent, preStorageMessage.downloadMetadata)
}


trait StorageApi extends PipelineStepService {
  def process(storeMessage: StorageMessage): StorageMessage

  final override def process(message: Any): Any = process(message.asInstanceOf[StorageMessage])
}

class Storage extends StorageApi {
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  override def process(storeMessage: StorageMessage): StorageMessage = {
    logger.debug(s"$id: processing $storeMessage")
    storeMessage
  }
}

object Storage extends PipelineStep {
  override val TYPE: String = "storage"
}