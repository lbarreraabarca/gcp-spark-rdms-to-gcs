package com.data.factory.adapters

import org.scalatest.Assertions._
import org.scalatest.FunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.RuntimeConfig


class TestSparkSessionFactory extends FunSuite {

    test("make local creates spark session as local") {
        //Arrange
        val factory = new SparkSessionFactory()

        //Act
        val session = factory.makeLocal()

        //Assert
        assert(session.isInstanceOf[SparkSession])
        assert(session.conf.get("spark.app.name") == "sparkTest")
        assert(session.conf.get("spark.sql.shuffle.partitions") == "1")

        session.close()

    }

    ignore("make cluster creates spark session as cluster") {
        //Arrange
        val factory = new SparkSessionFactory()

        //Act
        val session = factory.makeCluster()

        //Assert
        assert(session.isInstanceOf[SparkSession])
        assert(session.conf.get("spark.app.name") == "Pipeline")
        assert(session.conf.get("spark.network.timeout") == "600s")

        session.close()

    }

}