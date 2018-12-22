package net.degols.libs.filesgate.pipeline.download

import java.nio.file.Files.newOutputStream
import java.nio.file.Paths
import java.util.Date

import akka.stream.scaladsl.Sink
import akka.util.ByteString
import net.degols.libs.filesgate.orm.{FileMetadata, RawFileContent}
import net.degols.libs.filesgate.pipeline.matcher.MatcherMessage
import net.degols.libs.filesgate.pipeline.predownload.PreDownloadMessage
import net.degols.libs.filesgate.pipeline.{AbortStep, PipelineStep, PipelineStepMessage, PipelineStepService}
import net.degols.libs.filesgate.pipeline.prestorage.PreStorageMessage
import net.degols.libs.filesgate.utils.Tools
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json.JsObject

import scala.concurrent.{Await, Future}
import scala.util.Try


/**
  * Message sent through every PreDownloadApi before the actual download of a file
  * @param fileMetadata
  */
@SerialVersionUID(0L)
case class DownloadMessage(override val fileMetadata: FileMetadata, override val abort: Option[AbortStep], rawFileContent: Option[RawFileContent], downloadMetadata: Option[JsObject]) extends PipelineStepMessage(fileMetadata, abort)

object DownloadMessage {
  def from(preDownloadMessage: PreDownloadMessage): DownloadMessage = DownloadMessage(preDownloadMessage.fileMetadata, preDownloadMessage.abort, None, None)
}

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
  def process(downloadMessage: DownloadMessage): DownloadMessage

  final override def process(message: Any): Any = process(message.asInstanceOf[DownloadMessage])

}

class Download() extends DownloadApi {
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  /**
    * @param downloadMessage
    * @return
    */
  override def process(downloadMessage: DownloadMessage): DownloadMessage = {
    logger.debug(s"$id: processing $downloadMessage")
    downloadMessage
  }
}

object Download extends PipelineStep{
  override val TYPE: String = "download"
  override val MANDATORY: Boolean = true
}