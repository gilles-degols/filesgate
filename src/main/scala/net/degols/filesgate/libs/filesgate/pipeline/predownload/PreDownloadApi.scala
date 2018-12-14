package net.degols.filesgate.libs.filesgate.pipeline.predownload

import net.degols.filesgate.libs.filesgate.orm.FileMetadata
import net.degols.filesgate.libs.filesgate.pipeline.PipelineStepService
import org.slf4j.{Logger, LoggerFactory}

/**
  * @param reason the reason why we aborted the download
  * @param rescheduleSeconds if the value is filled, it means we do not want to download the file right now, but in
  *                          x seconds. The message won't go to any next pre-processing step. If the value is negative,
  *                          we will never re-schedule it.
  */
@SerialVersionUID(0L)
case class AbortDownload(reason: String, rescheduleSeconds: Option[Long])

/**
  * Message sent through every PreDownloadApi before the actual download of a file
  * @param fileMetadata
  * @param abortDownload if this value is received, we do not go any next pre-download stage
  */
@SerialVersionUID(0L)
case class PreDownloadMessage(fileMetadata: FileMetadata, abortDownload: Option[AbortDownload])

/**
  * Every pre-download process must extends this trait.
  */
trait PreDownloadApi extends PipelineStepService {
  /**
    * @param preDownloadMessage
    * @return
    */
  def process(preDownloadMessage: PreDownloadMessage): PreDownloadMessage

  final override def process(message: Any): Any = process(message.asInstanceOf[PreDownloadMessage])
}

class PreDownload extends PreDownloadApi {
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  override def process(preDownloadMessage: PreDownloadMessage): PreDownloadMessage = {
    logger.debug(s"$id: processing $preDownloadMessage")
    preDownloadMessage
  }
}

object PreDownload {
  val TYPE: String = "predownload"
}