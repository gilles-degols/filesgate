package net.degols.filesgate.service

import java.util
import java.util.Map
import collection.JavaConverters._

import com.google.inject.Inject
import com.typesafe.config.{Config, ConfigValue}
import play.api.Configuration

case class PipelineMetadata(id: String, stepIds: List[String], instancesByStep: Int)

class ConfigurationService @Inject()(val configuration: Configuration) {

  private def toMap(hashMap: AnyRef): Predef.Map[String, AnyRef] = hashMap.asInstanceOf[java.util.Map[String, AnyRef]].asScala.toMap
  private def toList(list: AnyRef): List[AnyRef] = list.asInstanceOf[java.util.List[AnyRef]].asScala.toList

  /**
    * The various pipelines defined in the configuration
    */
  val pipelines: List[PipelineMetadata] = {
    val set: util.Set[Map.Entry[String, ConfigValue]] = configuration.underlying.getConfig("filesgate.pipeline").entrySet()
    val res = toMap(set) map {
      case (key, value) =>
        val id = key
        val stepIds = value.asInstanceOf[Config].getList("step-ids").asInstanceOf[List[String]]
        val instances = value.asInstanceOf[Config].getInt("instances-by-step")
        PipelineMetadata(id, stepIds, instances)
    }

    res.toList
  }
}
