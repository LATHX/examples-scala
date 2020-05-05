package io.github.streamingwithflink.exercise

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object StreamWordCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // nc -lk 777 启动本地Socket服务
    val textDataStreamSource = env.socketTextStream("localhost", 777)

    // 逐一读取数据，打散之后wordcount
    val wordCountDataStream = textDataStreamSource.flatMap(_.split("\\s"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    // setParallelism 并行度
    wordCountDataStream.print().setParallelism(2)

    env.execute("stream word count")
  }
}
