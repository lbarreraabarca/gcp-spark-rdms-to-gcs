package com.data.factory.models

class ResponseSuccess(val value: String, val message: String) extends Response with Serializable {
    def isValid(): Boolean = true
    def getValue(): String = this.value
    def getMessage(): String = this.message
    override def toString(): String = this.value.concat(":").concat(this.message)
}