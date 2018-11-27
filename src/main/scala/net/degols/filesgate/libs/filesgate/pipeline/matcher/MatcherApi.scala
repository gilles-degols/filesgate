package net.degols.filesgate.libs.filesgate.pipeline.matcher

import net.degols.filesgate.libs.filesgate.orm.FileMetadata
import net.degols.filesgate.libs.filesgate.pipeline.PipelineStepService
import net.degols.filesgate.libs.filesgate.pipeline.download.{DownloadApi, DownloadMessage}
import org.slf4j.{Logger, LoggerFactory}

/**
  * Allows to filter any message and indicates if the file we should download belongs to the current Pipeline or not.
  * If more than one Pipeline is matched, select the first one.
  */
trait MatcherApi extends PipelineStepService {
  /**
    * @param fileMetadata
    * @return true if the current pipeline is meant to download the file, or not.
    */
  def process(fileMetadata: FileMetadata): Boolean

  override def process(message: Any): Any = process(message.asInstanceOf[FileMetadata])
}


class Matcher extends MatcherApi {
  override def id = "default.matcher"
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  /**
    * @param fileMetadata
    * @return
    */
  override def process(fileMetadata: FileMetadata): Boolean = {
    logger.debug(s"$id: processing $fileMetadata")
    true
  }
}