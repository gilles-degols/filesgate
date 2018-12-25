package net.degols.libs.filesgate.pipeline.predownload

import net.degols.libs.filesgate.orm.FileMetadata
import net.degols.libs.filesgate.pipeline.matcher.MatcherMessage
import net.degols.libs.filesgate.pipeline.{AbortStep, PipelineStep, PipelineStepMessage, PipelineStepService}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.{ExecutionContext, Future}

/**
  * Message sent through every PreDownloadApi before the actual download of a file
  * @param fileMetadata
  * @param abortDownload if this value is received, we do not go any next pre-download stage
  */
@SerialVersionUID(0L)
case class PreDownloadMessage(override val fileMetadata: FileMetadata, override val abort: Option[AbortStep]) extends PipelineStepMessage(fileMetadata, abort)

object PreDownloadMessage {
  def from(matcherMessage: MatcherMessage): PreDownloadMessage = PreDownloadMessage(matcherMessage.fileMetadata, matcherMessage.abort)
}

/**
  * Every pre-download process must extends this trait.
  */
trait PreDownloadApi extends PipelineStepService {
  /**
    * @param preDownloadMessage
    * @return
    */
  def process(preDownloadMessage: PreDownloadMessage): Future[PreDownloadMessage]

  final override def process(message: Any): Any = process(message.asInstanceOf[PreDownloadMessage])
}

class PreDownload(implicit val ec: ExecutionContext) extends PreDownloadApi {
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  override def process(preDownloadMessage: PreDownloadMessage): Future[PreDownloadMessage] = {
    Future{
      logger.debug(s"$id: processing $preDownloadMessage")
      preDownloadMessage
    }
  }
}

object PreDownload extends PipelineStep{
  override val TYPE: String = "predownload"
}