package net.degols.libs.filesgate.storage.systems.mongo

import com.google.inject.Inject
import javax.inject.Singleton
import net.degols.libs.filesgate.Tools
import net.degols.libs.cluster.{Tools => ClusterTools}
import net.degols.libs.filesgate.orm.{FileContent, FileMetadata}
import net.degols.libs.filesgate.pipeline.failurehandling.FailureHandlingMessage
import net.degols.libs.filesgate.storage.{SaveOperation, StorageMetadataApi, UpdateOperation}
import net.degols.libs.filesgate.utils.{FailedDatabaseOperation, FilesgateConfiguration, MetadataNotFound, Tools}
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json.{JsObject, Json}

import scala.concurrent.Future
import scala.util.{Failure, Success}
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

  override def upsert(failureHandlingMessage: FailureHandlingMessage): Future[UpdateOperation] = {
    Future{
      logger.info(s"Must upsert failure / abort for ${failureHandlingMessage}")
      val fileMetadata = failureHandlingMessage.initialMessage.fileMetadata
      val query = Json.obj(
        "_id" -> Json.obj("$oid" -> fileMetadata.id.substring(0,24)),
        "id" -> fileMetadata.id
      )
      val metadata = if(fileMetadata.metadata.keys.nonEmpty) Json.obj("metadata" -> fileMetadata.metadata)
      else Json.obj()

      // Generate pretty failure message
      val failure = failureHandlingMessage.exception match {
        case Some(err) =>
          Json.obj(
            "exception" -> ClusterTools.formatStacktrace(err).substring(0, 1000) // 1kB is enough information to keep
          )
        case None => // In this case, the only reason to be here is that we have an abort message
          val reschedule = failureHandlingMessage.outputMessage.get.abort.get.rescheduleSeconds match {
            case Some(value) => Json.obj("reschedule_s" -> Json.obj("$numberLong" -> value))
            case None => Json.obj()
          }

          Json.obj(
            "abort" -> failureHandlingMessage.outputMessage.get.abort.get.reason
          ) ++ reschedule
      }

      val set = Json.obj(
        "metadata" -> fileMetadata.metadata,
        "update_time" -> Json.obj("$date" -> Tools.datetime().getMillis),
        "downloaded" -> fileMetadata.downloaded,
        "failed" -> true
      ) ++ Json.obj("failure" -> failure) ++ metadata

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

  override def upsert(fileMetadata: FileMetadata): Future[UpdateOperation] = {
    Future{
      val query = Json.obj(
        "_id" -> Json.obj("$oid" -> fileMetadata.id.substring(0,24)),
        "id" -> fileMetadata.id
      )
      val metadata = if(fileMetadata.metadata.keys.nonEmpty) Json.obj("metadata" -> fileMetadata.metadata)
      else Json.obj()

      val set = Json.obj(
        "update_time" -> Json.obj("$date" -> Tools.datetime().getMillis),
        "downloaded" -> fileMetadata.downloaded
      ) ++ metadata

      val update = Json.obj(
        "$set" -> set,
        //"$unset" -> Json.obj("failed" -> 1),
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
    }.transformWith{
      case Success(value) => Future{value}
      case Failure(err) => Future{
        logger.error(s"Problem while updating the metadata: ${ClusterTools.formatStacktrace(err)}")
        throw err
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
