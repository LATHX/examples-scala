package io.github.streamingwithflink.exercise

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer010, FlinkKafkaProducer011}

object kafkaSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream = env.readTextFile("")

    val dataStream = inputStream.filter(_.contains("1"))

    dataStream.addSink(new FlinkKafkaProducer011[String]("localhost:9092", "sinkTest", new SimpleStringSchema()))
    env.execute("kafka stream sink")
  }
}
