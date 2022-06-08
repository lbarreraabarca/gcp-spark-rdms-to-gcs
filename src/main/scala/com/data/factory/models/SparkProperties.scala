package com.data.factory.models

import com.data.factory.exceptions.RequestException

import scala.collection.mutable.Map

class SparkProperties extends Serializable {
  var errorList: Map[String, String] = Map.empty[String, String]
  val objectMessage = "sparkProperties.%s"
  val errorMessage = "cannot be null or empty"

  var fetchSize: Integer = _

  def addError(field: String, message: String) = errorList += (field -> message)

  def this(fetchSize: Integer){
    this()
    if (fetchSize == null || fetchSize < 0) addError(String.format(objectMessage, "fetchSize"), errorMessage)
    else this.fetchSize = fetchSize
  }

  def isValid(): Boolean =
    if (!this.errorList.isEmpty) throw RequestException("this arguments cannot be empty or null:".concat(this.errorList.toString))
    else this.errorList.isEmpty
}
