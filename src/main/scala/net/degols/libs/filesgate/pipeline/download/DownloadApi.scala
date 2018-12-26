package net.degols.libs.filesgate.pipeline.download

import net.degols.libs.cluster.Tools
import net.degols.libs.cluster.messages.{BasicLoadBalancerType, ClusterInstance, Communication}
import net.degols.libs.filesgate.core.EngineLeader
import net.degols.libs.filesgate.orm.{FileMetadata, RawFileContent}
import net.degols.libs.filesgate.pipeline.predownload.PreDownloadMessage
import net.degols.libs.filesgate.pipeline.{AbortStep, PipelineStep, PipelineStepMessage, PipelineStepService}
import net.degols.libs.filesgate.utils.{Step, Tools}
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json.{JsObject, Json}

import scala.concurrent.{ExecutionContext, Future}


/**
  * Message sent through every PreDownloadApi before the actual download of a file
  * @param fileMetadata
  */
@SerialVersionUID(0L)
case class DownloadMessage(override val fileMetadata: FileMetadata, override val abort: Option[AbortStep], rawFileContent: Option[RawFileContent]) extends PipelineStepMessage(fileMetadata, abort)

object DownloadMessage {
  def from(preDownloadMessage: PreDownloadMessage): DownloadMessage = DownloadMessage(preDownloadMessage.fileMetadata, preDownloadMessage.abort, None)
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
  def process(downloadMessage: DownloadMessage): Future[DownloadMessage]

  final override def process(message: Any): Any = process(message.asInstanceOf[DownloadMessage])
}


class Download(implicit val ec: ExecutionContext, tools: Tools) extends DownloadApi{
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  /**
    * @param downloadMessage
    * @return
    */
  override def process(downloadMessage: DownloadMessage): Future[DownloadMessage] = {
    logger.info(s"Should download ${downloadMessage.fileMetadata.url}. Time is ${Tools.datetime()}")

    tools.downloadFileInMemory(downloadMessage.fileMetadata.url).map(rawDownloadFile => {
      val duration = rawDownloadFile.end.getTime - rawDownloadFile.start.getTime
      val content = new RawFileContent()
      val downloadMetadata = Json.obj(
        "download_time" -> Json.obj("$date" -> rawDownloadFile.start.getTime),
        "download_duration_ms" -> duration,
        "size_b" -> Json.obj("$numberLong" -> rawDownloadFile.size)
      )
      downloadMessage.fileMetadata.downloaded = true
      downloadMessage.fileMetadata.metadata = downloadMessage.fileMetadata.metadata ++ downloadMetadata
      DownloadMessage(downloadMessage.fileMetadata, downloadMessage.abort, Option(content))
    })
  }
}


object Download extends PipelineStep{
  override val TYPE: String = "download"
  override val MANDATORY: Boolean = true
  override val DEFAULT_STEP_NAME: String = "Core.Download"
  override val defaultStep: Option[Step] = {
    val fullStepName = Communication.fullActorName(EngineLeader.COMPONENT, EngineLeader.PACKAGE, DEFAULT_STEP_NAME)
    Option(Step(TYPE, fullStepName, BasicLoadBalancerType(1, ClusterInstance)))
  }
}