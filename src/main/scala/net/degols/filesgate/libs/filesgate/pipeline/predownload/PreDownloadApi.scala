package net.degols.filesgate.libs.filesgate.pipeline.predownload

import net.degols.filesgate.libs.filesgate.pipeline.PipelineStepService
import net.degols.filesgate.libs.filesgate.pipeline.poststorage.{PostStorageApi, PostStorageMessage}
import net.degols.filesgate.orm.{FileContent, FileMetadata}
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json.JsObject

/**
  * @param reason the reason why we aborted the download
  * @param rescheduleSeconds if the value is filled, it means we do not want to download the file right now, but in
  *                          x seconds. The message won't go to any next pre-processing step. If the value is negative,
  *                          we will never re-schedule it.
  */
case class AbortDownload(reason: String, rescheduleSeconds: Option[Long])

/**
  * Message sent through every PreDownloadApi before the actual download of a file
  * @param fileMetadata
  * @param abortDownload if this value is received, we do not go any next pre-download stage
  */
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

  override def process(message: Any): Any = process(message.asInstanceOf[PreDownloadMessage])
}

class PreDownload extends PreDownloadApi {
  override def id = "default.preDownload"
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  override def process(preDownloadMessage: PreDownloadMessage): PreDownloadMessage = {
    logger.debug(s"$id: processing $preDownloadMessage")
    preDownloadMessage
  }
}