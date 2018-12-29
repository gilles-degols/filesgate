package net.degols.libs.filesgate.pipeline.storage

import net.degols.libs.cluster.messages.{BasicLoadBalancerType, ClusterInstance, Communication}
import net.degols.libs.filesgate.core.EngineLeader
import net.degols.libs.filesgate.orm.{FileContent, FileMetadata}
import net.degols.libs.filesgate.pipeline.metadata.Metadata.TYPE
import net.degols.libs.filesgate.pipeline.prestorage.PreStorageMessage
import net.degols.libs.filesgate.pipeline.{AbortStep, PipelineStep, PipelineStepMessage, PipelineStepService}
import net.degols.libs.filesgate.storage.{StorageContentApi, StorageMetadataApi}
import net.degols.libs.filesgate.utils.Step
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json.JsObject

import scala.concurrent.{ExecutionContext, Future}

@SerialVersionUID(0L)
case class StorageMessage(override val fileMetadata: FileMetadata, override val abort: Option[AbortStep], fileContent: Option[FileContent]) extends PipelineStepMessage(fileMetadata, abort)

object StorageMessage {
  def from(preStorageMessage: PreStorageMessage): StorageMessage = StorageMessage(preStorageMessage.fileMetadata, preStorageMessage.abort, preStorageMessage.fileContent)
}


trait StorageApi extends PipelineStepService {
  def process(storeMessage: StorageMessage): Future[StorageMessage]

  final override def process(message: Any): Any = process(message.asInstanceOf[StorageMessage])
}

class Storage(dbService: StorageContentApi)(implicit val ec: ExecutionContext) extends StorageApi {
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  override def process(storeMessage: StorageMessage): Future[StorageMessage] = {
    dbService.upsert(storeMessage.fileMetadata, storeMessage.fileContent.get).map(res => {
      storeMessage
    })
  }
}

object Storage extends PipelineStep {
  override val TYPE: String = "storage"
  override val IMPORTANT_STEP: Boolean = true
  override val DEFAULT_STEP_NAME: String = "Core.Storage"
  override val defaultStep: Option[Step] = {
    val fullStepName = Communication.fullActorName(EngineLeader.COMPONENT, EngineLeader.PACKAGE, DEFAULT_STEP_NAME)
    Option(Step(TYPE, fullStepName, BasicLoadBalancerType(1, ClusterInstance)))
  }
}