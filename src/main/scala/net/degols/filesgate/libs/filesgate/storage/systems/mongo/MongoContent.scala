package net.degols.filesgate.storage.systems.mongo

import java.util.concurrent.Executors
import javax.inject.Singleton

import com.google.inject.Inject
import com.mongodb._
import com.mongodb.client.model._
import com.mongodb.client.{MongoClient => _, _}
import net.degols.filesgate.orm.FileContent
import net.degols.filesgate.service.Tools
import net.degols.filesgate.storage.StorageContentApi
import org.bson.Document
import org.bson.types.ObjectId
import org.joda.time.DateTime
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json.{JsObject, Json}
import services._

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}


/**
  * Handle every query linked to the storage of raw data.
  * @param conf
  */
@Singleton
class MongoContent @Inject()(conf: ConfigurationService, tools: Tools) extends StorageContentApi {
  private val logger: Logger = LoggerFactory.getLogger(getClass)
  implicit val executionContext: ExecutionContextExecutor = tools.executionContext
  private val mongoConfiguration = MongoConfiguration(
    uri = conf.configuration.get[String]("filesgate.storage.content.mongo.uri")
  )

  /**
    * Every access to the database go through this object
    */
  private val mongo: MongoUtils = new MongoUtils(conf, tools, mongoConfiguration)

  override def save(fileContent: FileContent, expectedSize: Option[Long]) = ???

  override def get(id: String) = ???

  override def delete(id: String) = ???
}
