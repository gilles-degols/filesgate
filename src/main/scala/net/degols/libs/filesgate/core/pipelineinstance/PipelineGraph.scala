package net.degols.libs.filesgate.core.pipelineinstance

import akka.{Done, NotUsed}
import akka.actor.ActorContext
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.util.Timeout

import scala.concurrent.duration._
import net.degols.libs.cluster.messages.Communication
import net.degols.libs.filesgate.core.PipelineStepStatus
import net.degols.libs.filesgate.orm.FileMetadata
import net.degols.libs.filesgate.pipeline.PipelineStepMessage
import net.degols.libs.filesgate.pipeline.datasource.{DataSource, DataSourceSeed}
import net.degols.libs.filesgate.pipeline.download.{Download, DownloadMessage}
import net.degols.libs.filesgate.pipeline.matcher.{Matcher, MatcherMessage}
import net.degols.libs.filesgate.pipeline.poststorage.{PostStorage, PostStorageMessage}
import net.degols.libs.filesgate.pipeline.predownload.{PreDownload, PreDownloadMessage}
import net.degols.libs.filesgate.pipeline.prestorage.{PreStorage, PreStorageMessage}
import net.degols.libs.filesgate.pipeline.storage.{Storage, StorageMessage}
import net.degols.libs.filesgate.utils.{FilesgateConfiguration, PipelineMetadata, Step}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
  * In charge of constructing the graph to fetch the data until writing them. Only create the graph, does not run it.
  */
class PipelineGraph(filesgateConfiguration: FilesgateConfiguration) {
  implicit var context: ActorContext = _
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  implicit val timeout = Timeout(120 seconds)

  var pipelineMetadata: PipelineMetadata = _
  var stepStatus: List[PipelineStepStatus] = _

  def constructSource: Source[FileMetadata, NotUsed] = ???

  def constructSource(source: Source[FileMetadata, NotUsed]): Source[FileMetadata, NotUsed] = ???

  def loadGraph(pipelineMetadata: PipelineMetadata, pipelineSteps: Map[String, PipelineStepStatus]): Unit = {
    implicit val materializer = ActorMaterializer()
    this.pipelineMetadata = pipelineMetadata
    this.stepStatus = pipelineSteps.values.toList

    // Various elements of the graph
    val source: Source[FileMetadata, NotUsed] = loadSourceSteps()
    val matcher = loadMatcherSteps()
    val preDownload = loadPreDownloadSteps()
    val download = loadDownloadSteps()
    val preStorage = loadPreStorageSteps()
    val storage = loadStorageSteps()
    val postStorage = loadPostStorageSteps()
    val sink = loadSinkSteps()

    // Now that we have raw flows, we need to add intermediate steps to convert the data
    source
      .map(m => MatcherMessage.from(m))
      .via(matcher)
      .via(preDownload)
      .via(download)
      .via(preStorage)
      .via(storage)
      .via(postStorage)
      .runWith(sink)
  }

  /**
    * Load the sink steps
    */
  def loadSinkSteps(): Sink[Any, Future[Done]] = {
    Sink.foreach(x => logger.debug(s"stuff: $x"))
  }

  /**
    * Load matcher steps
    */
  def loadMatcherSteps(): Flow[PipelineStepMessage, PipelineStepMessage, NotUsed] = {
    val stepWrappers = stepWrappersFromType(tpe = Matcher.TYPE)
    val flowStep: Flow[PipelineStepMessage, PipelineStepMessage, NotUsed] = loadAnySteps(stepWrappers.map(_._2))
    flowStep.map(m => PreDownloadMessage.from(m.asInstanceOf[MatcherMessage]))
  }

  /**
    * Load pre-download steps
    */
  def loadPreDownloadSteps(): Flow[PipelineStepMessage, PipelineStepMessage, NotUsed] = {
    val stepWrappers = stepWrappersFromType(tpe = PreDownload.TYPE)
    val flowStep = loadAnySteps(stepWrappers.map(_._2))
    flowStep.map(m => DownloadMessage.from(m.asInstanceOf[PreDownloadMessage]))
  }

  /**
    * Load download steps
    */
  def loadDownloadSteps(): Flow[PipelineStepMessage, PipelineStepMessage, NotUsed] = {
    val stepWrappers = stepWrappersFromType(tpe = Download.TYPE)
    val flowStep = loadAnySteps(stepWrappers.map(_._2))
    flowStep.map(m => PreStorageMessage.from(m.asInstanceOf[DownloadMessage]))
  }

  /**
    * Load pre-storage steps
    */
  def loadPreStorageSteps(): Flow[PipelineStepMessage, PipelineStepMessage, NotUsed] = {
    val stepWrappers = stepWrappersFromType(tpe = PreStorage.TYPE)
    val flowStep = loadAnySteps(stepWrappers.map(_._2))
    flowStep.map(m => StorageMessage.from(m.asInstanceOf[PreStorageMessage]))
  }

  /**
    * Load storage steps
    */
  def loadStorageSteps(): Flow[PipelineStepMessage, PipelineStepMessage, NotUsed] = {
    val stepWrappers = stepWrappersFromType(tpe = Storage.TYPE)
    val flowStep = loadAnySteps(stepWrappers.map(_._2))
    flowStep.map(m => PostStorageMessage.from(m.asInstanceOf[StorageMessage]))
  }

  /**
    * Load post-storage steps
    */
  def loadPostStorageSteps(): Flow[PipelineStepMessage, PipelineStepMessage, NotUsed] = {
    val stepWrappers = stepWrappersFromType(tpe = PostStorage.TYPE)
    val flowStep = loadAnySteps(stepWrappers.map(_._2))
    flowStep
  }

  /**
    * Load all sources and return a combination of them. If one is not received, fail.
    * @param stepWrappers
    */
  private def loadSourceSteps(): Source[FileMetadata, NotUsed] = {
    // Ask for the Sources, then merge them together. See https://doc.akka.io/docs/akka/2.5/stream/operators/Source-or-Flow/merge.html
    // We could use a MergePrioritized for the priority queue
    val sourceSteps = stepWrappersFromType(tpe = DataSource.TYPE).map(_._2)
    sourceSteps.map(pipelineStepStatus => {
      loadSource(pipelineStepStatus) match {
        case None => throw new Exception(s"Source $pipelineStepStatus cannot be found")
        case Some(s) => s
      }
    }).reduceLeft(_.merge(_))
  }

  /**
    * Load one source
    * @param pipelineStepStatus
    * @return
    */
  def loadSource(pipelineStepStatus: PipelineStepStatus): Option[Source[FileMetadata, NotUsed]] = {
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
    * Load any type of steps and merge flows of the same type.
    * Only exception: source and sink
    */
  private def loadAnySteps(pipelineStepStatuses: List[PipelineStepStatus]): Flow[PipelineStepMessage, PipelineStepMessage, NotUsed] = {
    pipelineStepStatuses.map(pipelineStepStatus => {
    Flow[PipelineStepMessage].mapAsync(5)(m => {
        (pipelineStepStatus.actorRef.get ? m)
          .map(_.asInstanceOf[PipelineStepMessage])
      }).filter(_.abort.isEmpty)
    }).foldLeft(Flow[PipelineStepMessage])(_.via(_))
  }

  /**
    * Return the Steps for a given type, and other information (actor ref, ...)
    */
  def stepWrappersFromType(tpe: String): List[(Step, PipelineStepStatus)] = {
    pipelineMetadata.steps.filter(_.tpe == tpe).flatMap(step => {
      stepStatus.find(_.fullName == step.name) match {
        case Some(stat) =>
          Option((step, stat))
        case None =>
          // Not all type are necessary, but if we are here, we should have found it in any case
          logger.error(s"We did not find the pipeline step for a known type ${tpe}")
          None
      }
    })
  }
}
