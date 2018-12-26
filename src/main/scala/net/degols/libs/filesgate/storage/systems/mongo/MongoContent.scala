package net.degols.libs.filesgate.storage.systems.mongo

import java.io.{BufferedInputStream, ByteArrayInputStream, InputStream}

import com.google.inject.Inject
import javax.inject.Singleton
import net.degols.libs.filesgate.orm.FileContent
import net.degols.libs.filesgate.storage.{SaveOperation, StorageContentApi, UpdateOperation}
import net.degols.libs.filesgate.utils.{FileNotFound, FilesgateConfiguration, Tools}
import org.bson.Document
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json.Json

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success, Try}


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

  override def upsert(fileContent: FileContent, expectedSize: Option[Long]): Future[UpdateOperation] = {
    Future{
      val query = Json.obj(
        "_id" -> Json.obj("$oid" -> fileContent.id.substring(0,24)),
        "id" -> Json.obj("id" -> fileContent.id)
      )
      val update = Json.obj(
        "$set" -> Json.obj("meta" -> fileContent.meta),
        "$setOnInsert" -> Json.obj(
          "_id" -> Json.obj("$oid" -> fileContent.id.substring(0,24)),
          "id" -> Json.obj("id" -> fileContent.id)
        )
      )
      val doc = Document.parse(update.toString)
      val set = doc.get("$set").asInstanceOf[Document]
      set.append("content", fileContent.raw)
      val setOnInsert = doc.get("$setOnInsert").asInstanceOf[Document]
      setOnInsert.append("content", fileContent.raw)

      doc.append("$set", set)
      doc.append("$setOnInsert", setOnInsert)

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
          new FileContent(id, bytes, meta)
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
