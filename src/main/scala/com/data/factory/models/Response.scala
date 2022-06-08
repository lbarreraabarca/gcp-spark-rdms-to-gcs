package com.data.factory.models

import scala.collection.mutable.Map

abstract class Response extends Serializable {
    def isValid(): Boolean
    def getValue(): String
    def getMessage(): String
    def toString(): String

}
