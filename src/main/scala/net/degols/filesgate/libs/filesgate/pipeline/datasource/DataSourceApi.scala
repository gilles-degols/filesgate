package net.degols.filesgate.libs.filesgate.pipeline.datasource

import net.degols.filesgate.libs.filesgate.orm.FileMetadata
import net.degols.filesgate.libs.filesgate.pipeline.{PipelineStep, PipelineStepService}
import org.slf4j.{Logger, LoggerFactory}

/**
  * There is not always a seed for the source, but having this message simplify the setup
  */
@SerialVersionUID(0L)
case class DataSourceSeed()

trait DataSourceApi extends PipelineStepService {
  def process(sourceSeed: DataSourceSeed): Iterator[FileMetadata]

  final override def process(message: Any): Any = process(message.asInstanceOf[DataSourceSeed])
}

class DataSource extends DataSourceApi {
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  override def process(sourceSeed: DataSourceSeed): Iterator[FileMetadata] = ???
}

object DataSource extends PipelineStep {
  override val TYPE: String = "datasource"
  override val MANDATORY: Boolean = true
}