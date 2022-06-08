package com.data.factory.ports

trait Queue {
    def publish(message: String, topic: String): Unit
}
