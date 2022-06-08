package com.data.factory.adapters

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

class SparkSessionFactory {

    def makeCluster(): SparkSession = {
        val sparkConf = new SparkConf()
        val appName = "Pipeline"
        sparkConf.set("spark.app.name", appName)
        sparkConf.set("spark.debug.maxToStringField", "200" )   
        sparkConf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
        sparkConf.set("spark.network.timeout", "600s")
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        spark
    }

    def makeLocal(): SparkSession = {
        val sparkConf = new SparkConf()
        val appName = "sparkTest"
        sparkConf.set("spark.app.name", appName)
        sparkConf.set("spark.sql.shuffle.partitions", "1")   
        val spark = SparkSession.builder().master("local").config(sparkConf).getOrCreate()

        return spark
    }

}