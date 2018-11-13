package net.degols.filesgate.libs.filesgate.pipeline.storage

import net.degols.filesgate.libs.filesgate.pipeline.PipelineStepService
import net.degols.filesgate.libs.filesgate.pipeline.prestorage.{PreStorageApi, PreStorageMessage}
import net.degols.filesgate.orm.FileMetadata
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json.JsObject

import scala.util.Try


case class StoreMessage(fileMetadata: FileMetadata)

trait StoreFileApi extends PipelineStepService {
  def process(storeMessage: StoreMessage): StoreMessage

  override def process(message: Any): Any = process(message.asInstanceOf[StoreMessage])
}

class StoreFile extends StoreFileApi {
  override def id = "default.storeFile"
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  override def process(storeMessage: StoreMessage): StoreMessage = {
    logger.debug(s"$id: processing $storeMessage")
    storeMessage
  }
}