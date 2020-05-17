package io.github.streamingwithflink.exercise

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object WindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // nc -lk 777 启动本地Socket服务
    val textDataStreamSource = env.socketTextStream("localhost", 777)

    // 统计15秒内的最小温度
    val wordCountDataStreamWindow = textDataStreamSource
      .map((_, 1))
      // watermark
//      .assignAscendingTimestamps(_._2)
      .keyBy(0)
//      .timeWindow(Time.seconds(5))
//      .window(new SlidingEventTimeWindows(Time.seconds(15),Time.seconds(5),Time.hours(8)))
      .reduce((data1, data2) => (data1._1, data1._2.min(data2._2)))

    // setParallelism 并行度
    wordCountDataStreamWindow.print().setParallelism(2)

    env.execute("stream word count")
  }
}
