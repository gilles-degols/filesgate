package net.degols.filesgate.libs.filesgate.pipeline.source

import net.degols.filesgate.libs.filesgate.orm.FileMetadata
import net.degols.filesgate.libs.filesgate.pipeline.PipelineStepService
import org.slf4j.{Logger, LoggerFactory}

/**
  * There is not always a seed for the source, but having this message simplify the setup
  */
case class SourceSeed()

trait SourceApi extends PipelineStepService {
  def process(sourceSeed: SourceSeed): Iterator[FileMetadata]

  final override def process(message: Any): Any = process(message.asInstanceOf[SourceSeed])
}

class Source extends SourceApi {
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  override def process(sourceSeed: SourceSeed): Iterator[FileMetadata] = ???
}

object Source {
  val TYPE: String = "source"
}