package net.degols.filesgate.libs.filesgate.core.pipelineinstance

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Source}
import net.degols.filesgate.libs.filesgate.orm.FileMetadata

/**
  * In charge of constructing the graph to fetch the data until writing them. Only create the graph, does not run it.
  */
class PipelineGraph {
  def constructSource: Source[FileMetadata, NotUsed] = ???

  def constructSource(source: Source[FileMetadata, NotUsed]): Source[FileMetadata, NotUsed] = ???
}
