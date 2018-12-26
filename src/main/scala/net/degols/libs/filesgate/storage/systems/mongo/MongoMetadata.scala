package net.degols.libs.filesgate.storage.systems.mongo

import com.google.inject.Inject
import javax.inject.Singleton
import net.degols.libs.filesgate.orm.{FileContent, FileMetadata}
import net.degols.libs.filesgate.storage.{SaveOperation, StorageMetadataApi, UpdateOperation}
import net.degols.libs.filesgate.utils.{FailedDatabaseOperation, FilesgateConfiguration, MetadataNotFound, Tools}
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json.{JsObject, Json}

import scala.concurrent.Future
import scala.xml.Document

/**
  * Handle every query linked to the storage of metadata linked to a file.
  *
  * @param conf
  */
@Singleton
class MongoMetadata @Inject()(conf: FilesgateConfiguration, tools: Tools) extends StorageMetadataApi {
  val DATABASE_NAME: String = "filesgate.metadata"
  val COLLECTION_NAME: String = "files"

  private val logger: Logger = LoggerFactory.getLogger(getClass)
  implicit val executionContext = tools.executionContext
  private val mongoConfiguration = MongoConfiguration(
    uri = conf.config.getString("filesgate.storage.metadata.mongo.uri")
  )

  /**
    * Every access to the database go through this object
    */
  private val mongo: MongoUtils = new MongoUtils(conf, tools, mongoConfiguration)

  override def update(fileMetadata: FileMetadata): Future[UpdateOperation] = {
    Future{
      val query = Json.obj(
        "_id" -> Json.obj("$oid" -> fileMetadata.id.substring(0,12)),
        "id" -> fileMetadata.id
      )

      val update = Json.obj("$set" -> Json.obj("metadata" -> fileMetadata.metadata))
      val op = mongo.updateOne(DATABASE_NAME, COLLECTION_NAME, query, update)
      if(op.wasAcknowledged()) {
        UpdateOperation()
      } else {
        throw new FailedDatabaseOperation(s"Failure to update the metadata ($fileMetadata) of a document.")
      }
    }
  }

  override def save(fileMetadata: FileMetadata): Future[SaveOperation] = {
    Future {
      val obj = Json.obj(
        "_id" -> Json.obj("$oid" -> fileMetadata.id.substring(0,12)),
        "id" -> fileMetadata.id,
        "url" -> fileMetadata.url,
        "metadata" -> fileMetadata.metadata
      )
      mongo.insertMany(DATABASE_NAME, COLLECTION_NAME, List(mongo.toDoc(obj)))
      SaveOperation()
    }
  }

  override def get(id: String): Future[FileMetadata] = {
    Future {
      val query = Json.obj(
        "_id" -> Json.obj("$oid" -> id.substring(0,12)),
        "id" -> id
      )

      mongo.findOne(DATABASE_NAME, COLLECTION_NAME, query, SortAsc()) match {
        case Some(res) =>
          val obj = mongo.toJson(res)
          val url = (obj \ "url").as[String]
          val metadata = (obj \ "metadata").as[JsObject]
          FileMetadata(url, metadata)
        case None =>
          throw new MetadataNotFound(s"File metadata not found for id: $id")
      }
    }
  }

  override def delete(id: String): Future[Boolean] = {
    Future {
      val query = Json.obj(
        "_id" -> Json.obj("$oid" -> id.substring(0,12)),
        "id" -> id
      )
      mongo.deleteMany(DATABASE_NAME, COLLECTION_NAME, query).wasAcknowledged()
    }
  }
}
