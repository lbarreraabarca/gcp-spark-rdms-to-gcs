package com.data.factory.exceptions

case class RequestException(message: String) extends Exception(message)