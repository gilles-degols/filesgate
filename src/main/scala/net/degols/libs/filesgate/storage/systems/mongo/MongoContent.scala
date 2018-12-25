package net.degols.libs.filesgate.storage.systems.mongo

import com.google.inject.Inject
import javax.inject.Singleton
import net.degols.libs.filesgate.orm.FileContent
import net.degols.libs.filesgate.storage.StorageContentApi
import net.degols.libs.filesgate.utils.{FilesgateConfiguration, Tools}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContextExecutor


/**
  * Handle every query linked to the storage of raw data.
  * @param conf
  */
@Singleton
class MongoContent @Inject()(conf: FilesgateConfiguration, tools: Tools) extends StorageContentApi {
  private val logger: Logger = LoggerFactory.getLogger(getClass)
  implicit val executionContext: ExecutionContextExecutor = tools.executionContext
  private val mongoConfiguration = MongoConfiguration(
    uri = conf.config.getString("filesgate.storage.content.mongo.uri")
  )

  /**
    * Every access to the database go through this object
    */
  private val mongo: MongoUtils = new MongoUtils(conf, tools, mongoConfiguration)

  override def save(fileContent: FileContent, expectedSize: Option[Long]) = ???

  override def get(id: String) = ???

  override def delete(id: String) = ???
}
