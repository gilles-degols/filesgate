package net.degols.libs.filesgate.storage.systems.mongo

import com.google.inject.Inject
import javax.inject.Singleton
import net.degols.libs.filesgate.Tools
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
  val DATABASE_NAME: String = "filesgateMetadata"
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

  override def upsert(fileMetadata: FileMetadata): Future[UpdateOperation] = {
    Future{
      val query = Json.obj(
        "_id" -> Json.obj("$oid" -> fileMetadata.id.substring(0,24)),
        "id" -> fileMetadata.id
      )
      val metadata = if(fileMetadata.metadata.keys.nonEmpty) Json.obj("metadata" -> fileMetadata.metadata)
      else Json.obj()

      val set = Json.obj(
        "metadata" -> fileMetadata.metadata,
        "update_time" -> Json.obj("$date" -> Tools.datetime().getMillis),
        "downloaded" -> fileMetadata.downloaded
      ) ++ metadata

      val update = Json.obj(
        "$set" -> set,
        "$setOnInsert" -> Json.obj(
          "_id" -> Json.obj("$oid" -> fileMetadata.id.substring(0,24)),
          "id" -> fileMetadata.id,
          "url" -> fileMetadata.url,
          "creation_time" -> Json.obj("$date" -> Tools.datetime().getMillis)
        )
      )

      val op = mongo.updateOne(DATABASE_NAME, COLLECTION_NAME, query, update, true)
      if(op.wasAcknowledged()) {
        UpdateOperation()
      } else {
        throw new FailedDatabaseOperation(s"Failure to update the metadata ($fileMetadata) of a document.")
      }
    }
  }

  override def get(id: String): Future[FileMetadata] = {
    Future {
      val query = Json.obj(
        "_id" -> Json.obj("$oid" -> id.substring(0,24)),
        "id" -> id
      )

      mongo.findOne(DATABASE_NAME, COLLECTION_NAME, query, SortAsc()) match {
        case Some(res) =>
          val obj = mongo.toJson(res)
          val fileMetadata = FileMetadata((obj \ "url").as[String])
          fileMetadata.downloaded = (obj \ "downloaded").asOpt[Boolean].getOrElse(false)
          fileMetadata.metadata = (obj \ "metadata").as[JsObject]
          fileMetadata
        case None =>
          throw new MetadataNotFound(s"File metadata not found for id: $id")
      }
    }
  }

  override def delete(id: String): Future[Boolean] = {
    Future {
      val query = Json.obj(
        "_id" -> Json.obj("$oid" -> id.substring(0,24)),
        "id" -> id
      )
      mongo.deleteMany(DATABASE_NAME, COLLECTION_NAME, query).wasAcknowledged()
    }
  }
}
