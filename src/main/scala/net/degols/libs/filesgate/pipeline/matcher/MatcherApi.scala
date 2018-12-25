package net.degols.libs.filesgate.pipeline.matcher

import net.degols.libs.filesgate.orm.FileMetadata
import net.degols.libs.filesgate.pipeline.{AbortStep, PipelineStep, PipelineStepMessage, PipelineStepService}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.{ExecutionContext, Future}

@SerialVersionUID(0L)
case class MatcherMessage(override val fileMetadata: FileMetadata, override val abort: Option[AbortStep]) extends PipelineStepMessage(fileMetadata: FileMetadata, abort: Option[AbortStep])

object MatcherMessage {
  def from(fileMetadata: FileMetadata): MatcherMessage = MatcherMessage(fileMetadata, None)
}

/**
  * Allows to filter any message and indicates if the file we should download belongs to the current Pipeline or not.
  * If more than one Pipeline is matched, select the first one.
  */
trait MatcherApi extends PipelineStepService {
  /**
    * @param fileMetadata
    * @return true if the current pipeline is meant to download the file, or not.
    */
  def process(matcherMessage: MatcherMessage): Future[MatcherMessage]

  final override def process(message: Any): Any = process(message.asInstanceOf[MatcherMessage])
}


class Matcher(implicit val ec: ExecutionContext) extends MatcherApi {
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  /**
    * @param fileMetadata
    * @return
    */
  override def process(matcherMessage: MatcherMessage): Future[MatcherMessage] = {
    Future {
      logger.debug(s"$id: processing $matcherMessage")
      matcherMessage
    }
  }
}

object Matcher extends PipelineStep{
  override val TYPE: String = "matcher"
}
