package net.degols.libs.filesgate.core.pipelineinstance

import akka.actor.ActorContext
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.util.Timeout
import akka.{Done, NotUsed}
import net.degols.libs.cluster.messages.Communication
import net.degols.libs.cluster.{Tools => ClusterTools}
import net.degols.libs.filesgate.core.PipelineStepStatus
import net.degols.libs.filesgate.orm.FileMetadata
import net.degols.libs.filesgate.pipeline.PipelineStepMessage
import net.degols.libs.filesgate.pipeline.datasource.{DataSource, DataSourceSeed}
import net.degols.libs.filesgate.pipeline.download.{Download, DownloadMessage}
import net.degols.libs.filesgate.pipeline.failurehandling.{FailureHandling, FailureHandlingMessage}
import net.degols.libs.filesgate.pipeline.matcher.{Matcher, MatcherMessage}
import net.degols.libs.filesgate.pipeline.metadata.{Metadata, MetadataMessage}
import net.degols.libs.filesgate.pipeline.postmetadata.{PostMetadata, PostMetadataMessage}
import net.degols.libs.filesgate.pipeline.predownload.{PreDownload, PreDownloadMessage}
import net.degols.libs.filesgate.pipeline.premetadata.{PreMetadata, PreMetadataMessage}
import net.degols.libs.filesgate.pipeline.prestorage.{PreStorage, PreStorageMessage}
import net.degols.libs.filesgate.pipeline.storage.{Storage, StorageMessage}
import net.degols.libs.filesgate.utils.{FilesgateConfiguration, PipelineMetadata, Step}
import org.joda.time.DateTime
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.concurrent.Futures
import play.api.libs.concurrent.Futures._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
  * Specific object to pass failure between steps, to store them at the end of the stream
  */
case class PipelineStepMessageWrapper(originalMessage: PipelineStepMessage, failure: Option[FailureHandlingMessage])


/**
  * In charge of constructing the graph to fetch the data until writing them. Only create the graph, does not run it.
  */
class PipelineGraph(filesgateConfiguration: FilesgateConfiguration) {
  implicit var context: ActorContext = _
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  implicit val futures: Futures = filesgateConfiguration.futures

  var pipelineMetadata: PipelineMetadata = _
  var stepStatus: List[PipelineStepStatus] = _

  /**
    * Start time of the stream
    */
  private var _streamStartTime: Option[DateTime] = None

  /**
    * Number of processed messages since the last _streamStartTime
    */
  private var _processedMessages: Long = 0L

  /**
    * Contains the graph created by loadGraph
    */
  private var _stream: Future[Done] = _

  /**
    * Return a simple future returning "Done". Useful to monitor its lifecycle. Can return null if the graph was not created.
    */
  def stream(): Future[Done] = _stream

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
    val preMetadata = loadPreMetadataSteps()
    val metadata = loadMetadataSteps()
    val postMetadata = loadPostMetadataSteps()
    val failureHandling = loadFailureHandlingSteps()
    val sink = loadSinkSteps()

    // For stats
    _streamStartTime = Option(new DateTime())
    _processedMessages = 0

    // Now that we have raw flows, we need to add intermediate steps to convert the data
    _stream = source
      .map(m => PipelineStepMessageWrapper(MatcherMessage.from(m), None))
      .via(matcher)
      .via(preDownload)
      .via(download)
      .via(preStorage)
      .via(storage)
      .via(preMetadata)
      .via(metadata)
      .via(postMetadata)
      .map(m => {
          _processedMessages += 1
          if(_processedMessages % 500 == 0) {
            val diff = new DateTime().getMillis - _streamStartTime.get.getMillis
            val speed: Long = math.round(_processedMessages / (diff / 1000.0))
            logger.info(s"${pipelineMetadata.id}: ${_processedMessages} messages in ${diff / 1000L} seconds ($speed messages/s).")
          }
          m
      })
      .via(failureHandling)
      .runWith(sink)

    // We do not add logs to the stream here, but one level above (in charge of asking the stream to start and monitor it)
  }

  /**
    * Load the sink steps
    */
  def loadSinkSteps(): Sink[Any, Future[Done]] = {
    Sink.ignore
  }

  /**
    * Load matcher steps
    */
  def loadMatcherSteps(): Flow[PipelineStepMessageWrapper, PipelineStepMessageWrapper, NotUsed] = {
    val stepWrappers = stepWrappersFromType(tpe = Matcher.TYPE)
    val flowStep: Flow[PipelineStepMessageWrapper, PipelineStepMessageWrapper, NotUsed] = loadAnySteps(stepWrappers.map(_._2))
    flowStep.map(m => {
      m.failure match {
        case Some(err) => m
        case None => PipelineStepMessageWrapper(PreDownloadMessage.from(m.originalMessage.asInstanceOf[MatcherMessage]), None)
      }
    })
  }

  /**
    * Load pre-download steps
    */
  def loadPreDownloadSteps(): Flow[PipelineStepMessageWrapper, PipelineStepMessageWrapper, NotUsed] = {
    val stepWrappers = stepWrappersFromType(tpe = PreDownload.TYPE)
    val flowStep = loadAnySteps(stepWrappers.map(_._2))
    flowStep.map(m => {
      m.failure match {
        case Some(err) => m
        case None => PipelineStepMessageWrapper(DownloadMessage.from(m.originalMessage.asInstanceOf[PreDownloadMessage]), None)
      }
    })
  }

  /**
    * Load download steps
    */
  def loadDownloadSteps(): Flow[PipelineStepMessageWrapper, PipelineStepMessageWrapper, NotUsed] = {
    val stepWrappers = stepWrappersFromType(tpe = Download.TYPE)
    val flowStep = loadAnySteps(stepWrappers.map(_._2))
    flowStep.map(m => {
      m.failure match {
        case Some(err) => m
        case None =>
          PipelineStepMessageWrapper(PreStorageMessage.from(m.originalMessage.asInstanceOf[DownloadMessage]), None)
      }
    })
  }

  /**
    * Load pre-storage steps
    */
  def loadPreStorageSteps(): Flow[PipelineStepMessageWrapper, PipelineStepMessageWrapper, NotUsed] = {
    val stepWrappers = stepWrappersFromType(tpe = PreStorage.TYPE)
    val flowStep = loadAnySteps(stepWrappers.map(_._2))
    flowStep.map(m => {
      m.failure match {
        case Some(err) => m
        case None => PipelineStepMessageWrapper(StorageMessage.from(m.originalMessage.asInstanceOf[PreStorageMessage]), None)
      }
    })
  }

  /**
    * Load storage steps
    */
  def loadStorageSteps(): Flow[PipelineStepMessageWrapper, PipelineStepMessageWrapper, NotUsed] = {
    val stepWrappers = stepWrappersFromType(tpe = Storage.TYPE)
    val flowStep = loadAnySteps(stepWrappers.map(_._2))
    flowStep.map(m => {
      m.failure match {
        case Some(err) => m
        case None => PipelineStepMessageWrapper(PreMetadataMessage.from(m.originalMessage.asInstanceOf[StorageMessage]), None)
      }
    })
  }

  /**
    * Load pre-metadata steps
    */
  def loadPreMetadataSteps(): Flow[PipelineStepMessageWrapper, PipelineStepMessageWrapper, NotUsed] = {
    val stepWrappers = stepWrappersFromType(tpe = PreMetadata.TYPE)
    val flowStep = loadAnySteps(stepWrappers.map(_._2))
    flowStep.map(m => {
      m.failure match {
        case Some(err) => m
        case None => PipelineStepMessageWrapper(MetadataMessage.from(m.originalMessage.asInstanceOf[PreMetadataMessage]), None)
      }
    })
  }

  /**
    * Load metadata steps
    */
  def loadMetadataSteps(): Flow[PipelineStepMessageWrapper, PipelineStepMessageWrapper, NotUsed] = {
    val stepWrappers = stepWrappersFromType(tpe = Metadata.TYPE)
    val flowStep = loadAnySteps(stepWrappers.map(_._2))
    flowStep.map(m => {
      m.failure match {
        case Some(err) => m
        case None => PipelineStepMessageWrapper(PostMetadataMessage.from(m.originalMessage.asInstanceOf[MetadataMessage]), None)
      }
    })
  }

  /**
    * Load post-metadata steps
    */
  def loadPostMetadataSteps(): Flow[PipelineStepMessageWrapper, PipelineStepMessageWrapper, NotUsed] = {
    val stepWrappers = stepWrappersFromType(tpe = PostMetadata.TYPE)
    val flowStep = loadAnySteps(stepWrappers.map(_._2))
    // In this case, no need to convert the message, the only remaining step is to handle failure, so we can keep the raw messages
    flowStep
  }

  /**
    * Load failure handling steps
    */
  def loadFailureHandlingSteps(): Flow[PipelineStepMessageWrapper, PipelineStepMessageWrapper, NotUsed] = {
    val stepWrappers = stepWrappersFromType(tpe = FailureHandling.TYPE)
    val flowStep = loadAnySteps(stepWrappers.map(_._2))
    // For this particular step, we want to have the "failure" in the PipelineStepMessageWrapper as the message itself
    Flow[PipelineStepMessageWrapper].filter(_.failure.isDefined).map(m => {
      logger.debug("Prepare message for FailureHandling")
      PipelineStepMessageWrapper(m.failure.get, None)
    }).via(flowStep)
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
    * Load any type of steps and merge flows of the same type. If some steps are missing this is not a problem.
    * Only exception: source and sink
    */
  private def loadAnySteps(pipelineStepStatuses: List[PipelineStepStatus]): Flow[PipelineStepMessageWrapper, PipelineStepMessageWrapper, NotUsed] = {
    pipelineStepStatuses.map(pipelineStepStatus => {
      // timeout for the Ask pattern
      implicit val timeout = Timeout(pipelineStepStatus.step.processingTimeout._1 millis)

      Flow[PipelineStepMessageWrapper].mapAsync(50)(m => {
        m.failure match {
          case Some(failure) =>
            logger.debug(s"Pipeline graph step: message ${failure.initialMessage} is a previous failure, skip the crurent step")
            Future{m}
          case None =>
            logger.debug(s"Pipeline graph step: send message ${m}")
            (pipelineStepStatus.actorRef.get ? m.originalMessage).withTimeout(timeout.duration._1 millis)
              .map(_.asInstanceOf[PipelineStepMessage]).transformWith{
              // We receive a raw message or a failure, but we need to return a PipelineStepMessageWrapper in any case
              case Success(res) =>
                Future{
                  // The abort field is a nice "skip" order given by the developer
                  res.abort match {
                    case Some(abortInfo) =>
                      val failure = FailureHandlingMessage.from(m.originalMessage, Some(res), None)
                      PipelineStepMessageWrapper(m.originalMessage, Some(failure))
                    case None =>
                      PipelineStepMessageWrapper(res, None)
                  }
                }
              case Failure(err) =>
                Future{
                  logger.error(s"${pipelineStepStatus.step.name} - Failure while processing the message $m, skip it and go further. Exception: \n ${ClusterTools.formatStacktrace(err)}")
                  val failure = FailureHandlingMessage.from(m.originalMessage, None, Some(err))
                  PipelineStepMessageWrapper(m.originalMessage, Some(failure))
                }
            }
        }
      })
    }).foldLeft(Flow[PipelineStepMessageWrapper])(_.via(_))
  }

  /**
    * Return the Steps for a given type, and other information (actor ref, ...)
    */
  def stepWrappersFromType(tpe: String): List[(Step, PipelineStepStatus)] = {
    pipelineMetadata.steps.filter(_.tpe == tpe).flatMap(step => {
      stepStatus.find(_.step.name == step.name) match {
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
