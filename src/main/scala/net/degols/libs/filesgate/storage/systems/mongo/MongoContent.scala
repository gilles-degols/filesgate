package net.degols.libs.filesgate.storage.systems.mongo

import java.nio.file.{Files, Paths}

import akka.stream.scaladsl.Source
import akka.util.ByteString
import com.google.inject.Inject
import javax.inject.Singleton
import net.degols.libs.filesgate.orm.{FileContent, FileMetadata}
import net.degols.libs.filesgate.storage.{StorageContentApi, UpdateOperation}
import net.degols.libs.filesgate.utils._
import org.bson.Document
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json.Json

import scala.concurrent.{ExecutionContextExecutor, Future}


/**
  * Handle every query linked to the storage of raw data.
  * @param conf
  */
@Singleton
class MongoContent @Inject()(conf: FilesgateConfiguration, tools: Tools) extends StorageContentApi {
  val DATABASE_NAME: String = "filesgateContent"
  val COLLECTION_NAME: String = "data"

  private val logger: Logger = LoggerFactory.getLogger(getClass)
  implicit val executionContext: ExecutionContextExecutor = tools.executionContext
  private val mongoConfiguration = MongoConfiguration(
    uri = conf.config.getString("filesgate.storage.content.mongo.uri")
  )

  /**
    * Every access to the database go through this object
    */
  private val mongo: MongoUtils = new MongoUtils(conf, tools, mongoConfiguration)

  override def upsert(fileMetadata: FileMetadata, downloadedFile: DownloadedFile): Future[UpdateOperation] = {
    Future{
      val query = Json.obj(
        "_id" -> Json.obj("$oid" -> fileMetadata.id.substring(0,24)),
        "id" -> fileMetadata.id
      )

      val metadata = if(downloadedFile.metadata.keys.nonEmpty) Json.obj("metadata" -> fileMetadata.metadata)
      else Json.obj()

      val set = Json.obj(
        "_id" -> Json.obj("$oid" -> fileMetadata.id.substring(0,24)),
        "id" -> fileMetadata.id
      ) ++ metadata


      val subDoc = Document.parse(set.toString())
      downloadedFile match {
        case fileToDisk: DownloadedFileToDisk =>
          fileToDisk.path match {
            case None =>
              throw new FileNotDownloaded(s"File for ${fileMetadata.id} was not downloaded.")
            case Some(path) =>
              val bytes = Files.readAllBytes(Paths.get(path))
              subDoc.append("content", bytes)
              // We do not remove the file here, but in the StorageApi. It means that you need to override the Storage
              // step if you want to have multiple Storage steps put together
          }
        case fileToMemory: DownloadedFileToMemory =>
          subDoc.append("content", fileToMemory.content)
      }
      val doc = new Document()
      doc.append("$set", subDoc)
      mongo.updateOneDoc(DATABASE_NAME, COLLECTION_NAME, query, doc, true)

      UpdateOperation()
    }
  }

  override def get(id: String): Future[FileContent] = {
    Future {
      val query = Json.obj(
        "_id" -> Json.obj("$oid" -> id.substring(0,24)),
        "id" -> id
      )
      mongo.findOne(DATABASE_NAME, COLLECTION_NAME, query, SortAsc()) match {
        case Some(res) =>
          val bytes: Array[Byte] = res.get("content", classOf[org.bson.types.Binary]).getData
          val meta = mongo.toJson(res.get("meta").asInstanceOf[Document])
          val b = ByteString.fromArray(bytes)
          new FileContent(id, b, meta)
        case None =>
          throw new FileNotFound(s"File not found: $id")
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
