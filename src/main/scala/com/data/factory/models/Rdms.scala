package com.data.factory.models

import com.data.factory.exceptions.RequestException

import scala.collection.mutable.Map

class Rdms extends Serializable {

  var errorList: Map[String, String] = Map.empty[String, String]
  val objectMessage = "rdms.%s"
  val errorMessage = "cannot be null or empty"

  var database: String = _
  var host: String = _
  var port: Int = _
  var schema: String = _
  var username: String = _
  var password: String = _
  var query: String = _

  def addError(field: String, message: String) = {
    errorList += (field -> message)
  }

  def this(database: String, host: String, port: Int, schema: String, username: String, password: String, query: String){
    this()
    if (database == null || database.isEmpty) addError(String.format(objectMessage, "database"), errorMessage)
    else this.database = database.toLowerCase
    if (host == null || host.isEmpty) addError(String.format(objectMessage, "host"), errorMessage)
    else this.host = host
    if (port == null || port <= 0) addError(String.format(objectMessage, "port"), errorMessage)
    else this.port = port
    if (schema == null || schema.isEmpty) addError(String.format(objectMessage, "schema"), errorMessage)
    else this.schema = schema
    if (username == null || username.isEmpty) addError(String.format(objectMessage, "username"), errorMessage)
    else this.username = username
    if (password == null || password.isEmpty) addError(String.format(objectMessage, "password"), errorMessage)
    else this.password = password
    if (query == null || query.isEmpty) addError(String.format(objectMessage, "query"), errorMessage)
    else this.query = query
  }

  def isValid(): Boolean = {
    if (!this.errorList.isEmpty) throw RequestException("this arguments cannot be empty or null:".concat(this.errorList.toString))
    else this.errorList.isEmpty
  }

  def getJdbcUrl(): String =
    this.database match {
      case "mysql" => "jdbc:mysql://%s:%s/%s?user=%s&password=%s".format(this.host, this.port.toString, this.schema, this.username, this.password)
      case _ => throw RequestException("Unimplemented database %s".format(this.database))
    }

  def getJdbcDriverClassName(): String =
    this.database match {
      case "mysql" => "com.mysql.cj.jdbc.Driver"
      case _ => throw RequestException("Unimplemented driver class name for database %s".format(this.database))
    }
}
