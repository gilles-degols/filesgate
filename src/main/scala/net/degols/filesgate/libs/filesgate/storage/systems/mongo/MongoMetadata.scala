package net.degols.filesgate.storage.systems.mongo

import javax.inject.Singleton

import com.google.inject.Inject
import net.degols.filesgate.orm.{FileContent, FileMetadata}
import net.degols.filesgate.service.Tools
import net.degols.filesgate.storage.{StorageContentApi, StorageMetadataApi}
import org.slf4j.{Logger, LoggerFactory}
import services.ConfigurationService

/**
  * Handle every query linked to the storage of metadata linked to a file.
  *
  * @param conf
  */
@Singleton
class MongoMetadata @Inject()(conf: ConfigurationService, tools: Tools) extends StorageMetadataApi {
  private val logger: Logger = LoggerFactory.getLogger(getClass)
  implicit val executionContext = tools.executionContext
  private val mongoConfiguration = MongoConfiguration(
    uri = conf.configuration.get[String]("filesgate.storage.metadata.mongo.uri")
  )

  /**
    * Every access to the database go through this object
    */
  private val mongo: MongoUtils = new MongoUtils(conf, tools, mongoConfiguration)

  override def update(fileMetadata: FileMetadata) = ???

  override def save(fileMetadata: FileMetadata) = ???

  override def get(id: String) = ???

  override def delete(id: String) = ???

  override def listToRecover(priority: Int, quantity: Int) = ???
}
