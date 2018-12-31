package net.degols.libs.filesgate.utils

import java.net.{URL, URLConnection}
import java.nio.file.Files.newOutputStream
import java.nio.file.Paths
import java.util.Date
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}

import akka.actor.{ActorContext, Cancellable}
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import javax.inject.Inject
import org.joda.time.DateTime
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.concurrent.Futures
import play.api.libs.concurrent.Futures._
import play.api.libs.json.{JsObject, Json}
import play.api.libs.ws.{WSClient, WSResponse}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}

/**
  * Result of a call to downloadFile. Children can contains the Array[Byte] in RAM, or a path to the given file where
  * we had to store the result
  */
sealed abstract class DownloadedFile(val url: String, val statusCode: Int, val start: Date, val end: Date, val size: Long, val metadata: JsObject = Json.obj()){
  override def toString: String = {
    val time = end.getTime - start.getTime
    s"DownloadedFile (status: $statusCode, time: $time ms): ${size/1024} kB @ $url"
  }
}

case class DownloadedFileToDisk(override val url: String, override val statusCode: Int, override val start: Date, override val end: Date, override val size: Long, override val metadata: JsObject, path: Option[String]) extends DownloadedFile(url, statusCode, start, end, size, metadata)
case class DownloadedFileToMemory(override val url: String, override val statusCode: Int, override val start: Date, override val end: Date, override val size: Long, override val metadata: JsObject, content: Option[ByteString]) extends DownloadedFile(url, statusCode, start, end, size, metadata)


class Tools @Inject()(implicit val mat: Materializer, implicit val fut: Futures, configurationService: FilesgateConfiguration, ws: WSClient) {
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  val threadPool: ExecutorService = Executors.newFixedThreadPool(20)
  implicit val executionContext: ExecutionContextExecutor =  ExecutionContext.fromExecutor(threadPool)

  /**
    * Download a large file that we write to the local disk directly
    * https://www.playframework.com/documentation/2.6.x/ScalaWS
    * @return
    */
  def downloadFileToDisk(url: String, destinationPath: String, connectionMaxSecond: Int = 5, downloadMaxSecond: Int = 10): Future[DownloadedFileToDisk] = {
    /**
      * TODO: We should benchmark this huge method vs the java one (way easier to read...):
    val u = new URL(downloadMessage.fileMetadata.url)
    val p = new File(dest)
    FileUtils.copyURLToFile(u, p)
      */
    val start: Date = new Date()
    val futureResponse: Future[WSResponse] = ws.url(url).withRequestTimeout(connectionMaxSecond seconds).withMethod("GET").stream().withTimeout(downloadMaxSecond seconds)

    var statusCode: Int = 0
    var totalSize: Int = 0
    futureResponse.flatMap{
      res =>
        val outputStream = newOutputStream(Paths.get(destinationPath))
        statusCode = res.status

        // The sink that writes to the output stream
        val sink = Sink.foreach[ByteString]{bytes =>
          totalSize += bytes.toArray.length
          outputStream.write(bytes.toArray)
        }

        // materialize and run the stream
        res.bodyAsSource.runWith(sink).andThen {
          case result =>
            // Close the output stream whether there was an error or not
            outputStream.close()
            outputStream.flush()
            // Get the result or rethrow the error
            result.get
        }
    }.map(done => {

      val end: Date = new Date

      val dest = if(statusCode >= 200 && statusCode <= 308) Option(destinationPath)
      else None

      if(dest.isEmpty) {
        totalSize = 0
      }

      val prettyResult = DownloadedFileToDisk(url, statusCode, start, end, totalSize, Json.obj(), dest)

      if (statusCode == 200) {
        val end: Date = new Date
        logger.debug(s"Download file (to disk): $url (file size size: ${(totalSize / 1024).toLong} kB at ${computeBandwidthRate(totalSize, start, end)})")
      } else {
        logger.warn(s"Download file (to disk) failure: $url. Status code: ${prettyResult.statusCode}")
      }
      prettyResult
    })
  }

  /**
    * Download a file and return an object to manipulate it directly in memory
    * @param url
    * @param connectionMaxSecond
    * @param downloadMaxSecond
    * @return
    */
  def downloadFileInMemory(url: String, connectionMaxSecond: Int = 5, downloadMaxSecond: Int = 10): Future[DownloadedFileToMemory] = {
    val start: Date = new Date()
    val download: Future[WSResponse] = ws.url(url).withRequestTimeout(connectionMaxSecond seconds).get().withTimeout(downloadMaxSecond seconds)

    // When we get the result, we want to convert it to
    download.map(res => {
      val end: Date = new Date()

      val bytes: Option[ByteString] = if(res.status >= 200 && res.status < 400) Option(res.bodyAsBytes)
      else None

      val fileSize: Long = if(bytes.isDefined) bytes.get.size
      else 0L

      val prettyResult = DownloadedFileToMemory(url, res.status, start, end, fileSize, Json.obj(), content = bytes)

      if (prettyResult.content.isDefined) {
        logger.debug(s"Download file to memory: $url")
      } else {
        logger.warn(s"Download file to memory - failure: $url. Status code: ${prettyResult.statusCode}")
      }

      prettyResult
    })
  }

  /**
    * Compute number of KB, MB, ... transferred by second.
    * @return
    */
  private def computeBandwidthRate(fileSize: Long, start: Date, end: Date): String = {
    val timeDifference: Double = math.max(0.00001,(end.getTime - start.getTime)/1000.0)

    if(fileSize / timeDifference > 1024L*1024L) s"${(fileSize / (timeDifference * 1024L * 1024L)).toInt} MB/s"
    else if(fileSize / timeDifference > 1024L) s"${(fileSize / (timeDifference * 1024L)).toInt} kB/s"
    else s"${(fileSize / timeDifference).toInt} B/s"
  }
}
