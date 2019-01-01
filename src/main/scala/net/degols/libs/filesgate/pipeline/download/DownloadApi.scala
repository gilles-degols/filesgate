package net.degols.libs.filesgate.pipeline.download

import java.io.{File, FileWriter}
import java.net.URL
import java.nio.file.Path

import akka.stream.scaladsl.Source
import akka.util.ByteString
import net.degols.libs.cluster.Tools
import net.degols.libs.cluster.messages.{BasicLoadBalancerType, ClusterInstance, Communication}
import net.degols.libs.filesgate.core.EngineLeader
import net.degols.libs.filesgate.orm.{FileContent, FileMetadata}
import net.degols.libs.filesgate.pipeline.predownload.PreDownloadMessage
import net.degols.libs.filesgate.pipeline.{AbortStep, PipelineStep, PipelineStepMessage, PipelineStepService}
import net.degols.libs.filesgate.utils.{DownloadedFile, Step, Tools}
import org.apache.commons.io.FileUtils
import org.joda.time.DateTime
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json.{JsObject, Json}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Random, Success, Try}


/**
  * Message sent through every PreDownloadApi before the actual download of a file
  * @param fileMetadata
  */
@SerialVersionUID(0L)
case class DownloadMessage(override val fileMetadata: FileMetadata, override val abort: Option[AbortStep], downloadedFile: Option[DownloadedFile]) extends PipelineStepMessage(fileMetadata, abort)

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


class Download(tools: Tools)(implicit val ec: ExecutionContext) extends DownloadApi{
  private val logger: Logger = LoggerFactory.getLogger(getClass)
  protected val r = Random

  override def process(downloadMessage: DownloadMessage): Future[DownloadMessage] = {
    logger.info(s"Should download ${downloadMessage.fileMetadata.url}. Time is ${Tools.datetime()}")

    // Random value to be sure to not have problems to download the same url on the same server
    val rand = r.nextInt()


    // Download the file in itsef. Maybe to disk or in memory, it depends of the step configuration
    val work: Future[DownloadedFile] = step.get.directory match {
      case Some(dir) => // We must download the file to the disk (slow, but does not use a lot of RAM & GC)
        val name = dir+"/download-file-"+rand+"-"+downloadMessage.fileMetadata.id
        tools.downloadFileToDisk(downloadMessage.fileMetadata.url, name)
      case None => // We must download the file in memory (fast, but use a lot of RAM & GC)
        tools.downloadFileInMemory(downloadMessage.fileMetadata.url)
    }

    // Now we format the pretty message
    val res = work.map(rawDownloadFile => {
      val duration = rawDownloadFile.end.getTime - rawDownloadFile.start.getTime
      val downloadMetadata = Json.obj(
        "download_time" -> Json.obj("$date" -> rawDownloadFile.start.getTime),
        "download_duration_ms" -> duration,
        "size_b" ->  rawDownloadFile.size
      )
      downloadMessage.fileMetadata.downloaded = true
      downloadMessage.fileMetadata.metadata = downloadMessage.fileMetadata.metadata ++ downloadMetadata
      DownloadMessage(downloadMessage.fileMetadata, downloadMessage.abort, Option(rawDownloadFile))
    })

    res
  }
}


object Download extends PipelineStep{
  override val TYPE: String = "download"
  override val IMPORTANT_STEP: Boolean = true
  override val DEFAULT_STEP_NAME: String = "Core.Download"
}