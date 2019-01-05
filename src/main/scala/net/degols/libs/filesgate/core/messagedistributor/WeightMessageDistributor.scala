package net.degols.libs.filesgate.core.messagedistributor

import net.degols.libs.filesgate.core.PipelineStepStatus
import net.degols.libs.filesgate.core.pipelineinstance.PipelineStepMessageWrapper
import net.degols.libs.filesgate.utils.Step
import org.joda.time.DateTime

import scala.util.Random

/**
  * Distribution of messages based on:
  *  -> Weighted-random distribution between stepStatuses, where the weights come from the statistics
  * So if an actor in general can process 1 message / 15 ms, we won't send the same number of messages to it than
  * to an actor able to process 1 message / 1 ms.
  *
  * This distributor is good if you have long-running tasks (like > 1s) in general. But it will produce hot-spots
  * if you have sometimes messages processed in 1ms.
  */
class WeightMessageDistributor extends MessageDistributor {
  val r = Random

  override def bestPipelineStepStatus(m: PipelineStepMessageWrapper, targetedStep: Step, stepStatuses: List[PipelineStepStatus]): Option[PipelineStepStatus] = {
    // In case we do not have every statistics, we compute the average processing time
    val processingTimes = stepStatuses.filter(_.actorStatistics.isDefined).map(_.actorStatistics.get.averageProcessingTime)
    val (totalAverageProcessingTime: Double, maxProcessingTime: Double) = if(processingTimes.nonEmpty) {
      ((processingTimes.sum * 1.0) / processingTimes.length, processingTimes.max)
    } else {
      (1f, 1f)
    }

    val currentDateTime = new DateTime().getMillis
    stepStatuses.sortBy(stepStatus => {
      val weight = stepStatus.actorStatistics match {
        case Some(stats) =>
          // If a process takes 10 ms instead of 5 ms to process messages, it should receive "2 times less" number of messages
          // But, it can happen that sometimes an actor takes a bit more time to process a message, and we do not want to alienate
          // this actor too much. We can easily have actors processing 1 message in 0.3ms, and another actor 0.6ms, etc.
          // So, we should take into account the number of processed messages, to be sure to have a meaningful average for each actor
          // And we should also re-try some messages if the last sent is old
          if(stats.lastMessageDateTime.getMillis < currentDateTime - 60*1000 || stats.totalProcessedMessage < 100) {
            totalAverageProcessingTime
          } else {
            stats.averageProcessingTime
          }
        case None => totalAverageProcessingTime
      }
      val normalizedWeight = weight / (maxProcessingTime * 1.0f)

      r.nextFloat() * normalizedWeight
    }).headOption
  }
}
