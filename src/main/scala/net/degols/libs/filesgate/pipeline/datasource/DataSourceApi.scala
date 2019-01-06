package net.degols.libs.filesgate.pipeline.datasource

import akka.NotUsed
import akka.stream.scaladsl.Source
import net.degols.libs.filesgate.orm.FileMetadata
import net.degols.libs.filesgate.pipeline.{PipelineStep, PipelineStepService}
import org.slf4j.{Logger, LoggerFactory}


/**
  * There is not always a seed for the source, but having this message simplify the setup
  */
@SerialVersionUID(0L)
case class DataSourceSeed()

trait DataSourceApi extends PipelineStepService {
  def process(sourceSeed: DataSourceSeed): Iterator[FileMetadata]
  def sourceProcess(sourceSeed: DataSourceSeed): Source[FileMetadata, Any] = Source.fromIterator(() => process(sourceSeed))

  final override def process(message: Any): Any = sourceProcess(message.asInstanceOf[DataSourceSeed])
}


object DataSource extends PipelineStep {
  override val TYPE: String = "datasource"
  override val IMPORTANT_STEP: Boolean = true
}