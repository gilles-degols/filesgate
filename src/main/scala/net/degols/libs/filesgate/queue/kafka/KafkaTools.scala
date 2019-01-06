package net.degols.libs.filesgate.queue.kafka

import java.util.Properties

import akka.actor.{ActorContext, ActorSystem}
import akka.kafka.ConsumerMessage.{CommittableMessage, CommittableOffset}
import akka.kafka.{ConsumerMessage, ConsumerSettings, Subscriptions}
import akka.kafka.scaladsl.Consumer
import akka.stream.scaladsl.Source
import org.apache.kafka.clients.consumer.{ConsumerConfig, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer

object KafkaTools {

  def getConsumerSettings(groupId: String, bootstrapServers: String)(implicit context: ActorContext): ConsumerSettings[String, String] = {
    ConsumerSettings(context.system, new StringDeserializer, new StringDeserializer)
      .withBootstrapServers(bootstrapServers)
      .withGroupId(groupId)
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      .withProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1")
      .withProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "500")
      .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
  }
  def consumer(consumerSettings: ConsumerSettings[String, String], topic: String): Source[ConsumerMessage.CommittableMessage[String, String], Consumer.Control] = {
    Consumer.committableSource(consumerSettings, Subscriptions.topics(topic))
  }

  /**
    * Only use this if you want to commit outside the stream
    * @param consumerSettings
    * @param topic
    * @param partition
    * @param offset
    */
  def commit(consumerSettings: ConsumerSettings[String, String], topic: String, partition: Int, offset: Long): Unit = {
    val topicPartition = new TopicPartition(topic, partition)
    val offsetAndMetadata = new OffsetAndMetadata(offset)
    val allOffsets = new java.util.HashMap[TopicPartition, OffsetAndMetadata]()
    allOffsets.put(topicPartition, offsetAndMetadata)
    consumerSettings.createKafkaConsumer().commitSync(allOffsets)
  }
}
