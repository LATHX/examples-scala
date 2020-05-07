package io.github.streamingwithflink.exercise

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaConsumer011}
// ./kafka-console-producer.sh --broker-list localhost:9092 --topic sensor
// Flink 自动保证kafka偏移量
object KafkaWordCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val properties = new Properties();
    properties.setProperty("bootstrap.servers", "ljlhaas.top:17092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value-serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("auto-offset.reset", "latest")

    val stream3 = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))

    stream3.print("stream3").setParallelism(1)

    env.execute("kafka test")
  }
}
