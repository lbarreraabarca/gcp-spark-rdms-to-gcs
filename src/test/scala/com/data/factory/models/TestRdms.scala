package com.data.factory.models

import org.json.JSONObject
import org.json4s.NoTypeHints
import org.json4s.native.Serialization
import org.json4s.native.Serialization.read
import org.scalatest._

import scala.collection.mutable.Map

class TestRdms extends FlatSpec{
  val errorMessage : String = "cannot be null or empty"

  def addError(errorList: Map[String, String],field: String, message: String) = {
    errorList += (field -> message)
  }

  "constructor" should "create a valid object when receive valid arguments" in {
    //Arrange
    val database: String = "mysql"
    val host: String = "localhost"
    val port: Int = 3306
    val schema: String = "test_db"
    val username: String = "username"
    val password: String = "password"
    val query: String = "c2VsZWN0ICogZnJvbSB0YWJsZQ=="

    //Act
    val rdbms = new Rdms(database, host, port, schema, username, password, query)

    //Assert
    assert(rdbms.isInstanceOf[Rdms])
    assert(rdbms.database == database)
    assert(rdbms.host == host)
    assert(rdbms.port == port)
    assert(rdbms.schema == schema)
    assert(rdbms.username == username)
    assert(rdbms.password == password)
    assert(rdbms.query == query)
    assert(rdbms.errorList.size == 0)
  }

  it should "create a valid object when receive valid json object." in {
    //Arrange
    val database: String = "mysql"
    val host: String = "localhost"
    val port: Int = 3306
    val schema: String = "test_db"
    val username: String = "username"
    val password: String = "password"
    val query: String = "c2VsZWN0ICogZnJvbSB0YWJsZQ=="

    val jsonRequest: JSONObject = new JSONObject()
    jsonRequest.put("database", database)
    jsonRequest.put("host", host)
    jsonRequest.put("port", port)
    jsonRequest.put("schema", schema)
    jsonRequest.put("username", username)
    jsonRequest.put("password", password)
    jsonRequest.put("query", query)

    implicit val formats = Serialization.formats(NoTypeHints)
    val rdbms: Rdms = read[Rdms](jsonRequest.toString)
    //Assert
    assert(rdbms.isInstanceOf[Rdms])
    assert(rdbms.database == database)
    assert(rdbms.host == host)
    assert(rdbms.port == port)
    assert(rdbms.schema == schema)
    assert(rdbms.username == username)
    assert(rdbms.password == password)
    assert(rdbms.query == query)
    assert(rdbms.errorList.size == 0)
  }
}
