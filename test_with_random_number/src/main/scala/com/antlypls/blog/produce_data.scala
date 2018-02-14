package com.antlypls.blog

import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Random
import java.util.{Properties}



object KafkaProducer {
  def main(args: Array[String]): Unit = {
    // Configurations for kafka consumer
    val kafkaBrokers = sys.env.get("KAFKA_BROKERS")
    val kafkaGroupId = sys.env.get("KAFKA_GROUP_ID")
    val kafkaTopic = sys.env.get("KAFKA_TOPIC")
    
    // Verify that all settings are set
    require(kafkaBrokers.isDefined, "KAFKA_BROKERS has not been set")
    require(kafkaGroupId.isDefined, "KAFKA_GROUP_ID has not been set")
    require(kafkaTopic.isDefined, "KAFKA_TOPIC has not been set")

    // Kafka parameters
    val kafkaParams = new Properties()
    kafkaParams.put("bootstrap.servers",kafkaBrokers.get)
    kafkaParams.put("key.serializer", classOf[StringSerializer])
    kafkaParams.put("value.serializer", classOf[StringSerializer])
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
    val producer = new KafkaProducer[String, String](kafkaParams)
    
    val rng = new Random(667)
    
    for (i <- 0 to 100) {
      val rnd = rng.nextDouble()
      var msg : String = "blubb"
      if (rnd < 0.5) {
        msg = "{\"action\":\"create\",\"timestamp\":"+rnd.toString+"}"
      } else {
        msg = "{\"action\":\"update\",\"timestamp\":"+rnd.toString+"}"
      }
      val data = new ProducerRecord[String,String](topicToWriteTo,msg)
      producer.send(data)
      println("sent " + msg + " to topic " + topicToWriteTo)
      Thread.sleep(1000) // wait for 1000 millisecond
    }
    
    producer.close()
   
  }
}
