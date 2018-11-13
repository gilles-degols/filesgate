package net.degols.filesgate.storage.systems.mongo

import com.google.inject.Inject
import com.mongodb.{MongoBulkWriteException, client}
import com.mongodb.client.model.{CreateCollectionOptions, IndexOptions, InsertManyOptions}
import com.mongodb.client._
import net.degols.filesgate.service.Tools
import org.bson.Document
import org.bson.types.ObjectId
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json.{JsObject, Json}
import services.ConfigurationService
import scala.collection.JavaConverters._

trait SortType {
  def json: JsObject
}
case class SortAsc() extends SortType {
  def json: JsObject = Json.obj("_id" -> 1)
}
case class SortDesc() extends SortType {
  def json: JsObject = Json.obj("_id" -> -1)
}

/**
  * Handle any connection to the database. Can be used directly by MongoContent and MongoMetadata, so not a singleton.
  * @param conf
  * @param tools
  */
class MongoUtils(conf: ConfigurationService, tools: Tools, mongoConfiguration: MongoConfiguration) {
  private val logger: Logger = LoggerFactory.getLogger(getClass)
  implicit val executionContext = tools.executionContext
  private val mongoClient: client.MongoClient = MongoClients.create(mongoConfiguration.uri)

  def getCollection(db: String, coll: String): MongoCollection[Document] = {
    val database: MongoDatabase = mongoClient.getDatabase(db)
    database.getCollection(coll)
  }

  /**
    * For performance purposes, we return Document and not JsObject for this method
    * @param db
    * @param coll
    * @param query
    * @return
    */
  def find(db: String, coll: String, query: JsObject, skip: Int, limit: Int, projection: JsObject, sortType: SortType = null): FindIterable[Document] = {
    if(sortType != null) getCollection(db, coll).find(toDoc(query)).skip(skip).limit(limit).projection(toDoc(projection)).sort(toDoc(sortType.json))
    else getCollection(db, coll).find(toDoc(query)).skip(skip).limit(limit).projection(toDoc(projection))
  }

  def findOne(db: String, coll: String, query: JsObject, sortType: SortType): Option[JsObject] = {
    toJsonList(find(db, coll, query, skip = 0, limit = 1, projection = Json.obj(), sortType = sortType).iterator()).headOption
  }

  def first(db: String, coll: String): Option[JsObject] = {
    toJsonList(find(db, coll, Json.obj(), skip = 0, limit = 1, projection = Json.obj(), sortType = SortAsc()).iterator()).headOption
  }

  def listDatabases: List[String] = {
    toStringList(mongoClient.listDatabaseNames().iterator())
  }

  def listCollections(db: String): List[String] = {
    toStringList(mongoClient.getDatabase(db).listCollectionNames().iterator())
  }

  def getCollectionIndexes(db: String, coll: String): List[JsObject] = {
    toJsonList(getCollection(db, coll).listIndexes().iterator())
  }

  def createCollection(db: String, coll: String, capped: Boolean, max: Long, maxSize: Long) = {
    val options = new CreateCollectionOptions()
    options.capped(capped)
    if(max > 0) options.maxDocuments(max)
    if(maxSize > 0) options.sizeInBytes(maxSize)
    mongoClient.getDatabase(db).createCollection(coll, options)
  }

  def createCollectionIndex(db: String, coll: String, key: JsObject, options: IndexOptions): String = {
    getCollection(db, coll).createIndex(toDoc(key), options)
  }

  def dropCollection(db: String, coll: String) = {
    getCollection(db, coll).drop()
  }

  /**
    * For performance purposes, we expect Document and not JsObject
    * @param db
    * @param coll
    * @param docs
    */
  def insertMany(db: String, coll: String, docs: List[Document]): Unit = {
    val options = new InsertManyOptions()
    options.ordered(false)
    options.bypassDocumentValidation(true)
    try{
      getCollection(db, coll).insertMany(docs.asJava, options)
    } catch {
      case ex: MongoBulkWriteException =>
      // As ordered is false, this is not a problem
      case x: Throwable =>
        logger.error("Error while inserting multiple documents")
        throw x
    }
  }

  def toDoc(obj: JsObject): Document = {
    Document.parse(obj.toString())
  }

  def toJson(obj: Document): JsObject = {
    Json.parse(obj.toJson.toString).as[JsObject]
  }

  def getObjectId(obj: JsObject): Option[ObjectId] = {
    (obj \ "_id" \ "$oid").asOpt[String] match {
      case None => None
      case Some(oid) => Option(new ObjectId(oid))
    }
  }

  def toStringList(iterator: MongoCursor[String]): List[String] = {
    var results = List.empty[String]
    while(iterator.hasNext) {
      results = List(iterator.next()) ::: results
    }
    results
  }

  def toJsonList(iterator: MongoCursor[Document]): List[JsObject] = {
    var results = List.empty[JsObject]
    while(iterator.hasNext) {
      results = List(toJson(iterator.next())) ::: results
    }
    results
  }
}
