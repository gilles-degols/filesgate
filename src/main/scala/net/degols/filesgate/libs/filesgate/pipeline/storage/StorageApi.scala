package net.degols.filesgate.libs.filesgate.pipeline.storage

import net.degols.filesgate.libs.filesgate.orm.FileMetadata
import net.degols.filesgate.libs.filesgate.pipeline.PipelineStepService
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Try


case class StoreMessage(fileMetadata: FileMetadata)

trait StorageApi extends PipelineStepService {
  def process(storeMessage: StoreMessage): StoreMessage

  final override def process(message: Any): Any = process(message.asInstanceOf[StoreMessage])
}

class Storage extends StorageApi {
  override def id = "default.storage"
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  override def process(storeMessage: StoreMessage): StoreMessage = {
    logger.debug(s"$id: processing $storeMessage")
    storeMessage
  }
}