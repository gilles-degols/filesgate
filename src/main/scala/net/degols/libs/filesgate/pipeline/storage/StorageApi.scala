package net.degols.libs.filesgate.pipeline.storage

import java.io.File

import net.degols.libs.cluster.messages.{BasicLoadBalancerType, ClusterInstance, Communication}
import net.degols.libs.filesgate.core.EngineLeader
import net.degols.libs.filesgate.orm.{FileContent, FileMetadata}
import net.degols.libs.filesgate.pipeline.prestorage.PreStorageMessage
import net.degols.libs.filesgate.pipeline.{AbortStep, PipelineStep, PipelineStepMessage, PipelineStepService}
import net.degols.libs.filesgate.storage.StorageContentApi
import net.degols.libs.filesgate.utils.{DownloadedFile, DownloadedFileToDisk, DownloadedFileToMemory, Step}
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json.JsObject

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

@SerialVersionUID(0L)
case class StorageMessage(override val fileMetadata: FileMetadata, override val abort: Option[AbortStep], downloadedFile: Option[DownloadedFile]) extends PipelineStepMessage(fileMetadata, abort)

object StorageMessage {
  def from(preStorageMessage: PreStorageMessage): StorageMessage = StorageMessage(preStorageMessage.fileMetadata, preStorageMessage.abort, preStorageMessage.downloadedFile)
}


trait StorageApi extends PipelineStepService {
  def process(storeMessage: StorageMessage): Future[StorageMessage]

  final override def process(message: Any): Any = process(message.asInstanceOf[StorageMessage])
}

class Storage(dbService: StorageContentApi)(implicit val ec: ExecutionContext) extends StorageApi {
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  protected def deleteDownloadFile(storageMessage: StorageMessage): Unit = {
    storageMessage.downloadedFile match {
      case Some(downloadedFile) =>
        downloadedFile match {
          case fileToDisk: DownloadedFileToDisk =>
            fileToDisk.path match {
              case Some(path) =>
                // If the file does not exist, the delete() return false
                new File(path).delete()
              case None =>
                // Nothing to do
            }
          case fileToMemory: DownloadedFileToMemory =>
            // Nothing to do
        }
      case None =>
        // Nothing to do
    }
  }

  override def process(storageMessage: StorageMessage): Future[StorageMessage] = {
    val res = dbService.upsert(storageMessage.fileMetadata, storageMessage.downloadedFile.get).map(res => {
      storageMessage
    })

    res.onComplete {
      case Success(res) => deleteDownloadFile(storageMessage)
      case Failure(err) => deleteDownloadFile(storageMessage)
    }

    res
  }
}

object Storage extends PipelineStep {
  override val TYPE: String = "storage"
  override val IMPORTANT_STEP: Boolean = true
  override val DEFAULT_STEP_NAME: String = "Core.Storage"
}