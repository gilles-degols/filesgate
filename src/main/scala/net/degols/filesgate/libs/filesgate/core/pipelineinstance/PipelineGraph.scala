package net.degols.filesgate.libs.filesgate.core.pipelineinstance

import akka.NotUsed
import akka.actor.ActorContext
import akka.pattern.ask
import akka.stream.scaladsl.{Flow, Source}
import akka.util.Timeout
import scala.concurrent.duration._
import net.degols.filesgate.libs.cluster.messages.Communication
import net.degols.filesgate.libs.filesgate.core.PipelineStepStatus
import net.degols.filesgate.libs.filesgate.orm.FileMetadata
import net.degols.filesgate.libs.filesgate.pipeline.datasource.{DataSource, DataSourceSeed}
import net.degols.filesgate.libs.filesgate.pipeline.matcher.Matcher
import net.degols.filesgate.libs.filesgate.utils.{FilesgateConfiguration, PipelineMetadata, Step}
import org.slf4j.{Logger, LoggerFactory}
import scala.concurrent.ExecutionContext.Implicits.global

import scala.util.{Failure, Success}

/**
  * In charge of constructing the graph to fetch the data until writing them. Only create the graph, does not run it.
  */
class PipelineGraph(filesgateConfiguration: FilesgateConfiguration) {
  var context: ActorContext = _
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  implicit val timeout = Timeout(120 seconds)

  def constructSource: Source[FileMetadata, NotUsed] = ???

  def constructSource(source: Source[FileMetadata, NotUsed]): Source[FileMetadata, NotUsed] = ???

  def loadGraph(pipelineMetadata: PipelineMetadata, pipelineSteps: Map[String, PipelineStepStatus]): Unit = {
    val sourceStepWrappers = stepWrappersFromType(pipelineMetadata, pipelineSteps, tpe = DataSource.TYPE)
    val source: Source[FileMetadata, NotUsed] = loadSources(sourceStepWrappers.map(_._2))

    val matcherStepWrappers = stepWrappersFromType(pipelineMetadata, pipelineSteps, tpe = Matcher.TYPE)
    val matcher = loadMatchers(matcherStepWrappers.map(_._2))
  }


  /**
    * Load all sources and return a combination of them. If one is not received, fail.
    * @param stepWrappers
    */
  private def loadSources(pipelineStepStatuses: List[PipelineStepStatus]): Source[FileMetadata, NotUsed] = {
    // Ask for the Sources, then merge them together. See https://doc.akka.io/docs/akka/2.5/stream/operators/Source-or-Flow/merge.html
    pipelineStepStatuses.map(pipelineStepStatus => {
      loadSource(pipelineStepStatus) match {
        case None => throw new Exception(s"Source ${pipelineStepStatus} cannot be found")
        case Some(s) => s
      }
    }).reduceLeft(_.merge(_))
  }

  private def loadSource(pipelineStepStatus: PipelineStepStatus): Option[Source[FileMetadata, NotUsed]] = {
    logger.debug("Send a message to get a source")
    Communication.sendWithReply(context.self, pipelineStepStatus.actorRef.get, DataSourceSeed()) match {
      case Success(res) =>
        logger.debug(s"Got a source: $res")
        Option(res.content.asInstanceOf[Source[FileMetadata, NotUsed]])
      case Failure(err) =>
        logger.error(s"Problem to get the Source: $err")
        None
    }
  }

  /**
    * Load matchers
    */
  private def loadMatchers(pipelineStepStatuses: List[PipelineStepStatus]) = {
    pipelineStepStatuses.map(pipelineStepStatus => {
      Flow[FileMetadata].mapAsync(5)(fileMetadata => {
        (pipelineStepStatus.actorRef.get ? fileMetadata)
          .filter(_.asInstanceOf[Boolean]) // Simply return a boolean saying if we accept a message or not
          .map(accepted => fileMetadata) // Return the object
      })
    }).reduceLeft(_.merge(_))
  }


  /**
    * Return the Steps for a given type, and other information (actor ref, ...)
    */
  def stepWrappersFromType(pipelineMetadata: PipelineMetadata, pipelineSteps: Map[String, PipelineStepStatus], tpe: String): List[(Step, PipelineStepStatus)] = {
    pipelineMetadata.steps.filter(_.tpe == tpe).flatMap(step => {
      pipelineSteps.find(_._2.fullName == step.name) match {
        case Some(stat) =>
          Option((step, stat._2))
        case None =>
          // Not all type are necessary, but if we are here, we should have found it in any case
          logger.error(s"We did not find the pipeline step for a known type ${tpe}")
          None
      }
    })
  }
}
