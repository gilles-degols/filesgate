package net.degols.libs.filesgate.utils

import java.nio.file.Files.newOutputStream
import java.nio.file.Paths
import java.util.Date
import java.util.concurrent.{ExecutorService, Executors}

import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import akka.util.ByteString
import javax.inject.Inject
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.concurrent.Futures
import play.api.libs.ws.{WSClient, WSResponse}
import play.api.libs.concurrent.Futures._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.{Failure, Success, Try}

/**
  * Result of a call to downloadFile. Children can contains the Array[Byte] in RAM, or a path to the given file where
  * we had to store the result
  */
abstract class DownloadedFile(url: String, statusCode: Int, start: Date, end: Date, size: Long){
  override def toString: String = {
    val time = end.getTime - start.getTime
    s"DownloadedFile (status: $statusCode, time: $time ms): ${size/1024} kB @ $url"
  }
}

case class DownloadedFileToDisk(url: String, statusCode: Int, start: Date, end: Date, size: Long, path: Option[String]) extends DownloadedFile(url, statusCode, start, end, size)
case class DownloadedFileToMemory(url: String, statusCode: Int, start: Date, end: Date, size: Long, content: Option[Array[Byte]]) extends DownloadedFile(url, statusCode, start, end, size)


class Tools @Inject()(implicit mat: Materializer, implicit val fut: Futures, configurationService: FilesgateConfiguration, ws: WSClient) {
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  val threadPool: ExecutorService = Executors.newFixedThreadPool(20)
  implicit val executionContext: ExecutionContextExecutor =  ExecutionContext.fromExecutor(threadPool)

  /**
    * Download a large file that we write to the local disk directly
    * https://www.playframework.com/documentation/2.6.x/ScalaWS
    * @return
    */
  def downloadFileToDisk(url: String, destinationPath: String, connectionMaxSecond: Int = 5, downloadMaxSecond: Int = 10): Future[DownloadedFileToDisk] = {
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

      val prettyResult = DownloadedFileToDisk(url, statusCode, start, end, totalSize, dest)

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

      val bytes = if(res.status >= 200 && res.status < 400) Option(res.bodyAsBytes.toArray)
      else None

      val fileSize: Long = if(bytes.isDefined) res.bodyAsBytes.toArray.length
      else 0L

      val prettyResult = DownloadedFileToMemory(url, res.status, start, end, fileSize, content = bytes)

      if (prettyResult.content.isDefined) {
        logger.debug(s"Download file to memory: $url (file size: ${(res.bodyAsBytes.size / 1024).toLong} kB at ${computeBandwidthRate(fileSize, start, end)})")
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
