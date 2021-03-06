package com.antlypls.blog

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.types.{StringType, StructType, TimestampType, DoubleType}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaSparkDemo {
  def main(args: Array[String]): Unit = {
    // Configurations for kafka consumer
    val kafkaBrokers = sys.env.get("KAFKA_BROKERS")
    val kafkaGroupId = sys.env.get("KAFKA_GROUP_ID")
    val kafkaTopic = sys.env.get("KAFKA_TOPIC")
    
    // Verify that all settings are set
    require(kafkaBrokers.isDefined, "KAFKA_BROKERS has not been set")
    require(kafkaGroupId.isDefined, "KAFKA_GROUP_ID has not been set")
    require(kafkaTopic.isDefined, "KAFKA_TOPIC has not been set")

    // Create Spark Session
    val spark = SparkSession
      .builder()
      .appName("KafkaSparkDemo")
      .getOrCreate()

    import spark.implicits._

    // Create Streaming Context and Kafka Direct Stream with provided settings and 10 seconds batches
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> kafkaBrokers.get,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> kafkaGroupId.get,
      "auto.offset.reset" -> "latest"
    )
    
    println(kafkaTopic)
    println(kafkaTopic.get)
    println(kafkaTopic.get.split(","))
    println(Array(kafkaTopic.get.split(",")))
    println(kafkaTopic.get.split(",").toList)
    val topics = kafkaTopic.get.split(",").toList
    val rawDataTopic = Array(topics(0))
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](rawDataTopic, kafkaParams)
    )
    
    // print out all Configurations
    println("----------------------")
    println("Kafka topics")
    println("----------------------")
    topics.foreach{ println }
    println("----------------------")
    println("Kafka topic to read raw data from")
    println("----------------------")
    println(rawDataTopic(0))
    println("----------------------")
    println("Kafka parameters")
    println("----------------------")
    kafkaParams.foreach{ println }
    
    
    
    // Define a schema for JSON data
    val schema = new StructType()
      .add("action", StringType)
      .add("timestamp", DoubleType)
    
    // Process batches:
    // Parse JSON and create Data Frame
    // Execute computation on that Data Frame and print result
    stream.foreachRDD { (rdd, time) =>
      val data = rdd.map(record => record.value)
      val json = spark.read.schema(schema).json(data)
      val result = json.groupBy($"action").agg(count("*").alias("count"))
      result.show
    }

    // Start Stream
    ssc.start()
    ssc.awaitTermination()
  }
}
