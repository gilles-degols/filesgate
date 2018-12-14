package net.degols.filesgate.libs.filesgate.orm

import play.api.libs.json.{JsObject, Json}

/**
  * Contains the metadata linked to a file.
  */
@SerialVersionUID(0L)
case class FileMetadata(val url: String, val metadata: JsObject = Json.obj()) {

}
