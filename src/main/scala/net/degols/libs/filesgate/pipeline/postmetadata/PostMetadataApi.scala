package net.degols.libs.filesgate.pipeline.postmetadata

import net.degols.libs.filesgate.orm.{FileMetadata, RawFileContent}
import net.degols.libs.filesgate.pipeline.metadata.MetadataMessage
import net.degols.libs.filesgate.pipeline.storage.StorageMessage
import net.degols.libs.filesgate.pipeline.{AbortStep, PipelineStep, PipelineStepMessage, PipelineStepService}
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json.JsObject

import scala.concurrent.{ExecutionContext, Future}

/**
  * Message sent through every PostStorageApi
  * @param fileMetadata
  * @param abortPostStorage if this value is received, we do not go any next post-storage stage
  */
@SerialVersionUID(0L)
case class PostMetadataMessage(override val fileMetadata: FileMetadata, override val abort: Option[AbortStep]) extends PipelineStepMessage(fileMetadata, abort)

object PostMetadataMessage {
  def from(metadataMessage: MetadataMessage): PostMetadataMessage = PostMetadataMessage(metadataMessage.fileMetadata, metadataMessage.abort)
}

/**
  * Every post-download process must extends this trait.
  */
trait PostMetadataApi extends PipelineStepService {
  /**
    * @param postMetadataMessage
    * @return
    */
  def process(postMetadataMessage: PostMetadataMessage): Future[PostMetadataMessage]

  final override def process(message: Any): Any = process(message.asInstanceOf[PostMetadataMessage])
}


class PostMetadata(implicit val ec: ExecutionContext) extends PostMetadataApi {
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  override def process(postMetadataMessage: PostMetadataMessage): Future[PostMetadataMessage] = {
    Future{
      logger.debug(s"$id: processing $postMetadataMessage")
      postMetadataMessage
    }
  }
}

object PostMetadata extends PipelineStep{
  override val TYPE: String = "postmetadata"
}