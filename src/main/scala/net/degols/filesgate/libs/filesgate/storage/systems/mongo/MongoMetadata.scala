package net.degols.filesgate.libs.filesgate.storage.systems.mongo

import javax.inject.Singleton
import com.google.inject.Inject
import net.degols.filesgate.libs.election.ConfigurationService
import net.degols.filesgate.libs.filesgate.orm.FileMetadata
import net.degols.filesgate.libs.filesgate.storage.StorageMetadataApi
import net.degols.filesgate.libs.filesgate.utils.{FilesgateConfiguration, Tools}
import org.slf4j.{Logger, LoggerFactory}

/**
  * Handle every query linked to the storage of metadata linked to a file.
  *
  * @param conf
  */
@Singleton
class MongoMetadata @Inject()(conf: FilesgateConfiguration, tools: Tools) extends StorageMetadataApi {
  private val logger: Logger = LoggerFactory.getLogger(getClass)
  implicit val executionContext = tools.executionContext
  private val mongoConfiguration = MongoConfiguration(
    uri = conf.config.getString("filesgate.storage.metadata.mongo.uri")
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
