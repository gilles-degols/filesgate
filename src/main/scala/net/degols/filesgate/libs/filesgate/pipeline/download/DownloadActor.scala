package net.degols.filesgate.libs.filesgate.pipeline.download

import net.degols.filesgate.libs.filesgate.orm.{FileMetadata, RawFileContent}
import net.degols.filesgate.libs.filesgate.pipeline.PipelineStepService
import net.degols.filesgate.libs.filesgate.pipeline.prestorage.PreStorageMessage
import org.slf4j.{Logger, LoggerFactory}


/**
  * Message sent through every PreDownloadApi before the actual download of a file
  * @param fileMetadata
  */
case class DownloadMessage(fileMetadata: FileMetadata)

/**
  * By default there will be only one Download step available, but this can be extended afterwards.
  * This class is a bit different as we receive a DownloadMessage and directly return a PreStorageMessage. The other classes
  * must return the same type to allow to pipe multiple steps of the same type together
  */
trait DownloadApi extends PipelineStepService {
  /**
    * @param downloadMessage
    * @return
    */
  def process(downloadMessage: DownloadMessage): PreStorageMessage

  final override def process(message: Any): Any = process(message.asInstanceOf[DownloadMessage])
}

class Download extends DownloadApi {
  override def id = "default.download"
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  /**
    * @param downloadMessage
    * @return
    */
  override def process(downloadMessage: DownloadMessage): PreStorageMessage = {
    logger.debug(s"$id: processing $downloadMessage")
    val rawFileContent = new RawFileContent()
    PreStorageMessage(downloadMessage.fileMetadata, rawFileContent, None)
  }
}