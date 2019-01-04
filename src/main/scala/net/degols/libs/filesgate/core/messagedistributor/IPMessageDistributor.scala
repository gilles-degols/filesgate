package net.degols.libs.filesgate.core.messagedistributor
import net.degols.libs.filesgate.core.PipelineStepStatus
import net.degols.libs.filesgate.core.pipelineinstance.PipelineStepMessageWrapper
import net.degols.libs.filesgate.pipeline.download.Download
import net.degols.libs.filesgate.utils.Step
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.Codecs

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.{Random, Try}

/**
  * This work distribution automatically map an url of file to download from server X always to the same actor on a server.
  * So if we have 3 servers, each of them has 5 actors, only 1 actor on each server will be allowed to process that message.
  *
  * If the number of actors change through time on a given node, the actor in charge of a destination server might change.
  */
class IPMessageDistributor extends MessageDistributor {
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  override def bestPipelineStepStatus(m: PipelineStepMessageWrapper, targetedStep: Step, stepStatuses: List[PipelineStepStatus]): Option[PipelineStepStatus] = {
    val destinationServer: String = m.originalMessage.fileMetadata.url.split("//").last.split("/").head.split(":").head
    if(targetedStep.tpe == Download.TYPE) {
      // We need the node -> stepStatus mapping
      val nodeStatusMapping: Map[String, List[PipelineStepStatus]] = stepStatuses.filter(_.actorRef.isDefined).map(stepStatus => _pipelineGraph.nodeFromActorRef(stepStatus.actorRef.get) -> stepStatus).groupBy(_._1).map(obj => obj._1 -> obj._2.map(_._2))

      // Select a random node
      Random.shuffle(nodeStatusMapping.keys.toList).headOption match {
        case Some(node) =>
          val nodeStepStatuses = nodeStatusMapping(node)
          val domainCode = convertDomainToInt(destinationServer).getOrElse(0)

          // We split the urls equally between the multiple actors. And to be sure to always have the same order, we need
          // to sort the actors.
          var i = 0
          var step = (math.pow(16, 6) / nodeStepStatuses.length).toInt
          val selectedStepStatus = nodeStepStatuses.sortBy(_.actorRef).find(stepStatus => {
            if(domainCode < i + step) {
              true
            } else {
              i += step
              false
            }
          })
          selectedStepStatus match {
            case Some(stepStatus) =>
              Option(stepStatus)
            case None => // An edge case probably, not very important
              nodeStepStatuses.sortBy(_.actorRef).headOption
          }
        case None => None
      }

    } else {
      // Nothing specific, a stepStatus at random to spread the load
      Random.shuffle(stepStatuses).headOption
    }
  }

  /**
    * Try to convert a domain (url part) to a "stable" int (always the same value) at least
    * @param domain
    * @return value between 0 & 16 exp(6)
    */
  def convertDomainToInt(domain: String): Option[Int] = {
    val param = Codecs.sha1(domain.getBytes).substring(0, 6)
    Try{Integer.parseInt(param, 16)}.toOption
  }
}
