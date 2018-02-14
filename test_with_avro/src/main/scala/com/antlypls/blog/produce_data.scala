package com.antlypls.blog

import java.util.{Properties, UUID}

import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
// import domain.User
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.avro.io._
import java.io.ByteArrayOutputStream

import org.apache.kafka.common.serialization.StringSerializer
// import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

import scala.util.Random
import java.io.File
import java.util.{Properties}

// input on avro encoding from
// https://dzone.com/articles/kafka-avro-scala-example

object myKafkaProducer {
  def main(args: Array[String]): Unit = {
    // Configurations for kafka consumer
    val kafkaBrokers = sys.env.get("KAFKA_BROKERS")
    val kafkaGroupId = sys.env.get("KAFKA_GROUP_ID")
    val kafkaTopic = sys.env.get("KAFKA_TOPIC")
    val kafkaMetadataBrokerList = sys.env.get("KAFKA_METADATA_BROKER_LIST")
    
    // Verify that all settings are set
    require(kafkaBrokers.isDefined, "KAFKA_BROKERS has not been set")
    require(kafkaGroupId.isDefined, "KAFKA_GROUP_ID has not been set")
    require(kafkaTopic.isDefined, "KAFKA_TOPIC has not been set")
    require(kafkaMetadataBrokerList.isDefined, "KAFKA_METADATA_BROKER_LIST has not been set")

    println(kafkaMetadataBrokerList.get)
    // Kafka parameters
    val kafkaParams = new Properties()
    kafkaParams.put("metadata.broker.list", kafkaMetadataBrokerList.get)
    kafkaParams.put("bootstrap.servers",kafkaBrokers.get)
    //kafkaParams.put("key.serializer", classOf[StringSerializer])
    //kafkaParams.put("value.serializer", classOf[StringSerializer])
    kafkaParams.put("serializer.class", "kafka.serializer.DefaultEncoder")
    kafkaParams.put("group.id", kafkaGroupId.get)
//       "auto.offset.reset" -> "latest"
    

    val topics = kafkaTopic.get.split(",").toList
    val topicToWriteTo = topics(0)
    
    // print out all Configurations
    println("----------------------")
    println("Kafka topics")
    println("----------------------")
    topics.foreach{ println }
    println("----------------------")
    println("Kafka topic to write to")
    println("----------------------")
    println(topicToWriteTo)
//     println("----------------------")
//     println("Kafka parameters")
//     println("----------------------")
//     kafkaParams.list{ System.out }
    
    
//     // Define a schema for JSON data
//     val schema = new StructType()
//       .add("action", StringType)
//       .add("timestamp", TimestampType)
//    
    val producer = new Producer[String, Array[Byte]](new ProducerConfig(kafkaParams))
    
    // Read avro schema file
    val schema: Schema = new Parser().parse(new File("schema.avsc"))
    
    val rng = new Random(667)
    
    for (i <- 0 to 100) {
      val rnd = rng.nextInt()
      
      // Create avro generic record object
      val genericUser: GenericRecord = new GenericData.Record(schema)
      // Put data in that generic record

      genericUser.put("id", rnd)
      genericUser.put("name", "sushil")
      genericUser.put("email", null)
      
      // Serialize generic record into byte array

      val writer = new SpecificDatumWriter[GenericRecord](schema)
      val out = new ByteArrayOutputStream()
      val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(out, null)
      writer.write(genericUser, encoder)
      encoder.flush()
      out.close()
      val serializedBytes: Array[Byte] = out.toByteArray()
      
      val queueMessage = new KeyedMessage[String, Array[Byte]](topicToWriteTo, serializedBytes) 
      producer.send(queueMessage)
      
      Thread.sleep(1000) // wait for 1000 millisecond
    }
    
    producer.close()
   
  }
}
