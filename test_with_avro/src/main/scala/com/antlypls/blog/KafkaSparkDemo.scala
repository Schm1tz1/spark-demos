package com.antlypls.blog

import java.io.File
import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.io.DatumReader
import org.apache.avro.io.Decoder
import org.apache.avro.specific.SpecificDatumReader
// import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DecoderFactory


import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord, GenericRecordBuilder}

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import kafka.serializer.{StringDecoder,DefaultDecoder}
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
      "value.deserializer" -> classOf[ByteArrayDeserializer],
      "group.id" -> kafkaGroupId.get,
      "auto.offset.reset" -> "latest",
      "zookeeper.connect" -> "localhost:2181" // WARN: hard-coded, but should work
    )
    
    println(kafkaTopic)
    println(kafkaTopic.get)
    println(kafkaTopic.get.split(","))
    println(Array(kafkaTopic.get.split(",")))
    println(kafkaTopic.get.split(",").toList)
    val topics = kafkaTopic.get.split(",").toList
    val rawDataTopic = Array(topics(0))
    val stream = KafkaUtils.createDirectStream[String, Array[Byte]](
      ssc,
      PreferConsistent,
      Subscribe[String, Array[Byte]](rawDataTopic, kafkaParams)
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
    
    
    
    
    
    // Process batches:
    // Parse JSON and create Data Frame
    // Execute computation on that Data Frame and print result
    stream.foreachRDD { (rdd, time) =>
      if (rdd != null) {
        
        // Deserialize and create generic record
        val rdd_data = rdd.map{ record => parseAVROtoString(record.value) }
        
//         rdd_data.foreach( x => { println( (x, schema) ) } )
        rdd_data.foreach{ println }
        
  //       val json = spark.read.schema(schema).json(data)
  //       val result = json.groupBy($"action").agg(count("*").alias("count"))
//         result.show
      }
    }

    // Start Stream
    ssc.start()
    ssc.awaitTermination()
  }
  
  // example for such code:
  // https://community.hortonworks.com/articles/33275/receiving-avro-messages-through-kafka-in-a-spark-s.html
  
  def parseAVROtoString(rawByteArray : Array[Byte]) : String = {
    try {
      if ( rawByteArray.isEmpty ) {
        println("no message")
        "Empty"
      } else {
        // Read avro schema file
        val schema: Schema = new Parser().parse(new File("schema.avsc"))
        val reader = new GenericDatumReader[GenericRecord](schema)
        val decoder = DecoderFactory.get.binaryDecoder(rawByteArray, null)
        val msg = reader.read(null, decoder)
        msg.get("id").toString
      }
    } catch {
      case e: Exception => None
      null
    }
    
  }
  
}


