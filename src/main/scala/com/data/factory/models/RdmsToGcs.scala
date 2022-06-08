package com.data.factory.models

import com.data.factory.exceptions.RequestException

import scala.collection.mutable.Map

class RdmsToGcs extends Serializable {
  var errorList: Map[String, String] = Map.empty[String, String]
  val objectMessage = "rdmsToGcs.%s"
  val errorMessage = "cannot be null or empty"

  var rdms: Rdms = _
  var gcs: Gcs = _
  var sparkProperties: SparkProperties = _
  var processDate: String = _
  var nextSteps: String = _

  def addError(field: String, message: String) = {
    errorList += (field -> message)
  }

  def this(rdms: Rdms, gcs: Gcs, nextSteps: String, sparkProperties: SparkProperties, processDate: String){
    this()
    if (rdms.isValid()) this.rdms = rdms
    if (gcs.isValid()) this.gcs = gcs
    if (sparkProperties.isValid()) this.sparkProperties = sparkProperties

    if (processDate == null || processDate.isEmpty) addError("processDate", errorMessage)
    else if (!isValidDate(processDate)) addError("processDate", errorMessage)
    else this.processDate = processDate

    if (nextSteps == null) this.nextSteps = ""
    else this.nextSteps = nextSteps
  }

  def isValidDate(datePartition: String): Boolean =
    ("""^[\d]{4}-[\d]{2}-[\d]{2}$""".r findFirstMatchIn datePartition).isDefined

  def isValid(): Boolean = {
    if (!this.errorList.isEmpty) throw RequestException("this arguments cannot be empty or null:".concat(this.errorList.toString))
    else this.errorList.isEmpty
  }
}
