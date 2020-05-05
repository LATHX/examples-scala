package io.github.streamingwithflink.exercise

import org.apache.flink.api.scala._

object WordCount {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val inputPath = "/Users/ljl/Documents/workspace/examples-scala/src/main/scala/io/github/streamingwithflink/exercise/wordCount.txt"
    val inputDataSet = env.readTextFile(inputPath)

    // 分词统计
    val workCountDataSet= inputDataSet.flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)

    workCountDataSet.print()
  }
}
