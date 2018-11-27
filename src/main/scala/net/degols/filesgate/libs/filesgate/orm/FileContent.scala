package net.degols.filesgate.libs.filesgate.orm

import play.api.libs.json.JsObject

/**
  * Contain the raw data of a FileMetadata. Depending on the size of the file, it might be in RAM or on disk.
  */
class FileContent(val json: JsObject) {


}
