package net.degols.libs.filesgate.core.pipelineinstance

import akka.actor.{ActorContext, ActorRef}
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, MergePrioritized, Sink, Source}
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
import net.degols.libs.filesgate.utils._
import org.joda.time.DateTime
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.concurrent.Futures
import play.api.libs.concurrent.Futures._

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Random, Success}

/**
  * Specific object to pass failure between steps, to store them at the end of the stream.
  * We also use it to store the information about each Actor used to handle the different steps, this is useful
  * to send messages between actors on the same jvm. Ideally, the pipelineInstance itself must on be the same
  * jvm, but in that case we would not be able to scale one pipeline instance on multiple nodes. So we will not
  * try to put the pipeline instance on a specific node, as long as the same node process the Download, PreStorage
  * and Storage steps (necessary if you store locally the file)
  */
case class PipelineStepMessageWrapper(var originalMessage: PipelineStepMessage, var failure: Option[FailureHandlingMessage]) {
  private val _pipelineStepStatuses: ListBuffer[PipelineStepStatus] = ListBuffer[PipelineStepStatus]()
  def addPipelineStepStatus(pipelineStepStatus: PipelineStepStatus): Unit = _pipelineStepStatuses.append(pipelineStepStatus)
  def pipelineStepStatuses: ListBuffer[PipelineStepStatus] = _pipelineStepStatuses

}


/**
  * In charge of constructing the graph to fetch the data until writing them. Only create the graph, does not run it.
  */
class PipelineGraph(filesgateConfiguration: FilesgateConfiguration) {
  implicit var context: ActorContext = _
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  implicit val futures: Futures = filesgateConfiguration.futures

  private var _pipelineMetadata: PipelineMetadata = _
  private var _pipelineInstanceMetadata: PipelineInstanceMetadata = _
  def setPipelineMetadata(pipelineMetadata: PipelineMetadata): Unit = {
    this._pipelineMetadata = pipelineMetadata
  }

  def setPipelineInstanceMetadata(pipelineInstanceMetadata: PipelineInstanceMetadata): Unit = {
    this._pipelineInstanceMetadata = pipelineInstanceMetadata
  }

  private var _steps: List[Step] = _
  private var _pipelineStepStatuses: Map[Step, List[PipelineStepStatus]] = Map.empty[Step, List[PipelineStepStatus]]
  def setSteps(steps: List[Step]): Unit = _steps = steps
  def setPipelineStepStatuses(step: Step, pipelineStepStatuses: List[PipelineStepStatus]): Unit = {
    _pipelineStepStatuses = _pipelineStepStatuses ++ Map(step -> pipelineStepStatuses)
    generateNodeAndStepStatus()
  }

  private var _nodeAndStepStatus: Map[String, List[PipelineStepStatus]] = _

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



  /**
    * You need to call setPipelineStepStatuses() before calling this method
    * @param pipelineMetadata
    * @param pipelineInstanceMetadata
    */
  def loadGraph(): Unit = {
    implicit val materializer = ActorMaterializer()

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
            logger.info(s"${_pipelineMetadata.id} / ${_pipelineInstanceMetadata.numberId}: ${_processedMessages} messages in ${diff / 1000L} seconds ($speed messages/s).")
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
    val flowStep: Flow[PipelineStepMessageWrapper, PipelineStepMessageWrapper, NotUsed] = loadAnySteps(stepWrappers)
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
    val flowStep = loadAnySteps(stepWrappers)
    flowStep.map(m => {
      m.failure match {
        case Some(err) => m
        case None =>
          m.originalMessage = DownloadMessage.from(m.originalMessage.asInstanceOf[PreDownloadMessage])
          m
      }
    })
  }

  /**
    * Load download steps
    */
  def loadDownloadSteps(): Flow[PipelineStepMessageWrapper, PipelineStepMessageWrapper, NotUsed] = {
    val stepWrappers = stepWrappersFromType(tpe = Download.TYPE)
    val flowStep = loadAnySteps(stepWrappers)
    flowStep.map(m => {
      m.failure match {
        case Some(err) => m
        case None =>
          m.originalMessage = PreStorageMessage.from(m.originalMessage.asInstanceOf[DownloadMessage])
          m
      }
    })
  }

  /**
    * Load pre-storage steps
    */
  def loadPreStorageSteps(): Flow[PipelineStepMessageWrapper, PipelineStepMessageWrapper, NotUsed] = {
    val stepWrappers = stepWrappersFromType(tpe = PreStorage.TYPE)
    val flowStep = loadAnySteps(stepWrappers)
    flowStep.map(m => {
      m.failure match {
        case Some(err) => m
        case None =>
          m.originalMessage = StorageMessage.from(m.originalMessage.asInstanceOf[PreStorageMessage])
          m
      }
    })
  }

  /**
    * Load storage steps
    */
  def loadStorageSteps(): Flow[PipelineStepMessageWrapper, PipelineStepMessageWrapper, NotUsed] = {
    val stepWrappers = stepWrappersFromType(tpe = Storage.TYPE)
    val flowStep = loadAnySteps(stepWrappers)
    flowStep.map(m => {
      m.failure match {
        case Some(err) => m
        case None =>
          m.originalMessage = PreMetadataMessage.from(m.originalMessage.asInstanceOf[StorageMessage])
          m
      }
    })
  }

  /**
    * Load pre-metadata steps
    */
  def loadPreMetadataSteps(): Flow[PipelineStepMessageWrapper, PipelineStepMessageWrapper, NotUsed] = {
    val stepWrappers = stepWrappersFromType(tpe = PreMetadata.TYPE)
    val flowStep = loadAnySteps(stepWrappers)
    flowStep.map(m => {
      m.failure match {
        case Some(err) => m
        case None =>
          m.originalMessage = MetadataMessage.from(m.originalMessage.asInstanceOf[PreMetadataMessage])
          m
      }
    })
  }

  /**
    * Load metadata steps
    */
  def loadMetadataSteps(): Flow[PipelineStepMessageWrapper, PipelineStepMessageWrapper, NotUsed] = {
    val stepWrappers = stepWrappersFromType(tpe = Metadata.TYPE)
    val flowStep = loadAnySteps(stepWrappers)
    flowStep.map(m => {
      m.failure match {
        case Some(err) => m
        case None =>
          m.originalMessage = PostMetadataMessage.from(m.originalMessage.asInstanceOf[MetadataMessage])
          m
      }
    })
  }

  /**
    * Load post-metadata steps
    */
  def loadPostMetadataSteps(): Flow[PipelineStepMessageWrapper, PipelineStepMessageWrapper, NotUsed] = {
    val stepWrappers = stepWrappersFromType(tpe = PostMetadata.TYPE)
    val flowStep = loadAnySteps(stepWrappers)
    // In this case, no need to convert the message, the only remaining step is to handle failure, so we can keep the raw messages
    flowStep
  }

  /**
    * Load failure handling steps
    */
  def loadFailureHandlingSteps(): Flow[PipelineStepMessageWrapper, PipelineStepMessageWrapper, NotUsed] = {
    val stepWrappers = stepWrappersFromType(tpe = FailureHandling.TYPE)
    val flowStep = loadAnySteps(stepWrappers)
    // For this particular step, we want to have the "failure" in the PipelineStepMessageWrapper as the message itself
    Flow[PipelineStepMessageWrapper].filter(_.failure.isDefined).map(m => {
      logger.debug("Prepare message for FailureHandling")
      m.originalMessage = m.failure.get
      m.failure = None
      m
    }).via(flowStep)
  }

  /**
    * Load all sources and return a combination of them. If one is not received, fail.
    * @param stepWrappers
    */
  private def loadSourceSteps(): Source[FileMetadata, NotUsed] = {
    // Ask for the Sources, then merge them together. See https://doc.akka.io/docs/akka/2.5/stream/operators/Source-or-Flow/merge.html
    // We could use a MergePrioritized for the priority queue
    // Note: For the source there is no load balancing available
    val sourceSteps = stepWrappersFromType(tpe = DataSource.TYPE).map(step => _pipelineStepStatuses(step)).map(stepStatuses => {
      if(stepStatuses.size > 1) {
        logger.error("Only one instance of a SourceStep is allowed! Other instances will not be considered")
      }
      stepStatuses.head // We should always have one so it can fail
    })

    val rawSources: List[Source[FileMetadata, NotUsed]] = sourceSteps.map(pipelineStepStatus => {
      loadSource(pipelineStepStatus) match {
        case None => throw new Exception(s"Source $pipelineStepStatus cannot be found")
        case Some(s) => s
      }
    })

    // Multiplying everything by 100, as it seems the priority is used to create a buffer: https://github.com/akka/akka/issues/25823
    val priorityList: List[Int] = sourceSteps.map(_.step.priority*100)

    val mergedSources: Source[FileMetadata, NotUsed] = if(rawSources.size == 1) rawSources.head
    else if(rawSources.size == 2) Source.combine(rawSources.head, rawSources(1))(numInputs => MergePrioritized(priorityList))
    else Source.combine(rawSources.head, rawSources(1), rawSources:_*)(numInputs => MergePrioritized(priorityList))

    mergedSources
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
    * For load balancing, we just need to select one pipelineStepStatus at random. But for the PreStorage & Storage
    * steps, the receiving actors MUST be on the same node (but not necessarily on the same jvm), as they will need
    * to access a local downloaded file. If there is no actor available, we put the message as failure. If there is no
    * other actor available, we just need to stop the graph and re-create it.
    * Special case: if there are multiple Download steps, they must all be on the same jvm as well.
    * Other special case: to send the message to the appropriate first Download step, we need to be sure there are
    * related PreStorage & Storage steps on the same node, otherwise we would download a lot of messages for nothing.
    * @param pipelineStepStatuses stepStatus for the targeted step to work
    * @param allPipelineStepStatuses all step status of
    * @return
    */
  def preferredPipelineStepStatus(message: PipelineStepMessageWrapper, targetedStep: Step): Option[PipelineStepStatus] = {
    // The first download step is enough
    val downloadStatus: Option[PipelineStepStatus] = stepWrappersFromType(Download.TYPE).headOption match {
      case Some(downloadStep) =>
        _pipelineStepStatuses.getOrElse(downloadStep, List.empty[PipelineStepStatus]).headOption
      case None => None
    }
    val stepStatuses = _pipelineStepStatuses(targetedStep)

    if(downloadStatus.isDefined && (targetedStep.tpe == Download.TYPE || targetedStep.tpe == PreStorage.TYPE || targetedStep.tpe == Storage.TYPE)) {
      // We target jvm with the same node as the one used for the download step
      val downloadNode = nodeFromActorRef(downloadStatus.get.actorRef.get)
      val matchingStatuses = stepStatuses.filter(stepStatus => downloadNode == nodeFromActorRef(stepStatus.actorRef.get))
      if (matchingStatuses.nonEmpty) {
        Random.shuffle(matchingStatuses).headOption
      } else {
        None
      }
    } else if(downloadStatus.isEmpty && targetedStep.tpe == Download.TYPE) {
      // We need to be sure that we have steps for the PreStorage and Storage
      downloadWithColocation(targetedStep)
    } else {
      Random.shuffle(stepStatuses).headOption
    }
  }

  private def nodeFromActorRef(actorRef: ActorRef): String = actorRef.toString.split("//")(1).split("/").head.split(":").head


  /**
    * Load any type of steps (of the same type) and merge flows of the same type. If some steps are missing this is not a problem.
    * Only exception: source and sink
    */
  private def loadAnySteps(steps: List[Step]): Flow[PipelineStepMessageWrapper, PipelineStepMessageWrapper, NotUsed] = {
    steps.map(step => {
      // timeout for the Ask pattern
      implicit val timeout: Timeout = Timeout(step.processingTimeout._1 millis)

      Flow[PipelineStepMessageWrapper].mapAsync(50)(m => {
        m.failure match {
          case Some(failure) =>
            logger.debug(s"Pipeline graph step: message ${failure.initialMessage} is a previous failure, skip the crurent step")
            Future{m}
          case None =>
            // Select a random actorref to send the message. This is a stupid but efficient load balancing
            // We do not use the pipelineStepStatuses received as input, as they can change through the life of the graph
            // we must rather use _pipelineStepStatuses which is updated when we find new actor ref on the cluster.
            val pipelineStepStatus = preferredPipelineStepStatus(m, step).get

            logger.debug(s"Pipeline graph step: send message ${m} to $pipelineStepStatus")
            m.addPipelineStepStatus(pipelineStepStatus)
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
    * Compute the mapping node -> List[PipelineStepStatus]. For performance purposes this should be called once every time
    * the pipelineStepStatuses are updated
    */
  def generateNodeAndStepStatus(): Unit = {
    _nodeAndStepStatus = _pipelineStepStatuses.values.flatten
      .map(stepStatus => nodeFromActorRef(stepStatus.actorRef.get) -> stepStatus)
      .groupBy(_._1)
      .map(obj => obj._1 -> obj._2.map(_._2).toList)
  }

  def getStepStatuses(node: String): List[PipelineStepStatus] = {
    _nodeAndStepStatus.getOrElse(node, List.empty[PipelineStepStatus])
  }

  /**
    * Return the Steps for a given type. We verify that we have at least one actor for each step otherwise we throw
    * an error as we shouldn't start the graph.
    * We can have multiple Step for a given type.
    */
  def stepWrappersFromType(tpe: String): List[Step] = {
    _pipelineInstanceMetadata.steps.filter(_.tpe == tpe).map(step => {
      val matchingStepStatuses: List[PipelineStepStatus] = _pipelineStepStatuses.getOrElse(step, List.empty[PipelineStepStatus])
      if(matchingStepStatuses.isEmpty){
        throw new MissingStepActor(s"Missing step statuses for type $tpe and pipeline ${_pipelineMetadata.id}")
      }
      step
    })
  }

  /**
    * Can we still run the graph or should we stop it? Also used to start it at first.
    * This should be combined with the PipelineInstance tests
    */
  def areActorsColocated(): Boolean = {
    // First we verify that we have colocated actors for the download / prestorage / storage steps
    val steps = stepWrappersFromType(Download.TYPE)
    val okForColocation: Boolean = if(steps.nonEmpty) {
      downloadWithColocation(steps.head).isDefined
    } else {
      true
    }

    okForColocation
  }

  /**
    * Verify if there are download / prestorage / storage colocated. This is called for every message to find a first download
    * step (so this needs to be efficient and avoid involving too much CPU/GC). But this is also used to detect if we can start a graph.
    * @return
    */
  def downloadWithColocation(targetedStep: Step): Option[PipelineStepStatus] = {
    val stepStatuses = _pipelineStepStatuses(targetedStep)

    val otherDownloadSteps = stepWrappersFromType(Download.TYPE).dropWhile(_ != targetedStep).filterNot(_ == targetedStep) // takes elements after the given step
    val preStorageSteps = stepWrappersFromType(PreStorage.TYPE)
    val storageSteps = stepWrappersFromType(Storage.TYPE)

    // We need to find a valid download step, which means the one having "matching" prestorage/storage steps on the same jvm
    val downloadStepStatus = Random.shuffle(stepStatuses).find(stepStatus => {
      val downloadNode = nodeFromActorRef(stepStatus.actorRef.get)
      // if we have PreStorage or Storage steps, we must be sure to have an actor for each of them on the same node
      val nodeStepStatuses = getStepStatuses(downloadNode)
      val otherDownloadOnSameNode = otherDownloadSteps.forall(downStep => nodeStepStatuses.exists(_.step == downStep))
      val preStorageOnSameNode = preStorageSteps.forall(storageStep => nodeStepStatuses.exists(_.step == storageStep))
      val storageOnSameNode = storageSteps.forall(preStorageStep => nodeStepStatuses.exists(_.step == preStorageStep))
      otherDownloadOnSameNode && preStorageOnSameNode && storageOnSameNode
    })

    if(downloadStepStatus.isEmpty){
      logger.error("We found no download step running on the same node as every pre-storage and storage step.")
    }

    downloadStepStatus
  }
}
