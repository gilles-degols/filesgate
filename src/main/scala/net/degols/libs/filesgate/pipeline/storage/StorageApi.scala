package net.degols.libs.filesgate.pipeline.storage

import net.degols.libs.filesgate.orm.{FileMetadata, RawFileContent}
import net.degols.libs.filesgate.pipeline.prestorage.PreStorageMessage
import net.degols.libs.filesgate.pipeline.{AbortStep, PipelineStep, PipelineStepMessage, PipelineStepService}
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json.JsObject

import scala.concurrent.{ExecutionContext, Future}

@SerialVersionUID(0L)
case class StorageMessage(override val fileMetadata: FileMetadata, override val abort: Option[AbortStep], rawFileContent: Option[RawFileContent], downloadMetadata: Option[JsObject]) extends PipelineStepMessage(fileMetadata, abort)

object StorageMessage {
  def from(preStorageMessage: PreStorageMessage): StorageMessage = StorageMessage(preStorageMessage.fileMetadata, preStorageMessage.abort, preStorageMessage.rawFileContent, preStorageMessage.downloadMetadata)
}


trait StorageApi extends PipelineStepService {
  def process(storeMessage: StorageMessage): Future[StorageMessage]

  final override def process(message: Any): Any = process(message.asInstanceOf[StorageMessage])
}

class Storage(implicit val ec: ExecutionContext) extends StorageApi {
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  override def process(storeMessage: StorageMessage): Future[StorageMessage] = {
    Future{
      logger.debug(s"$id: processing $storeMessage")
      storeMessage
    }
  }
}

object Storage extends PipelineStep {
  override val TYPE: String = "storage"
}