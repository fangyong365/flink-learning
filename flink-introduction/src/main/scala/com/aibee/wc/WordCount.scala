package com.aibee.wc

import org.apache.flink.api.scala._

/**
  * @author shkstart 
  * @create 2020-10-25 11:41 
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val inputPath = "F:\\Hadoop\\IdeaProject\\flink-learning\\flink-introduction\\src\\main\\resources\\hello.txt"

    val inputDS: DataSet[String] = env.readTextFile(inputPath)

    val wordCountDS: AggregateDataSet[(String, Int)] = inputDS.flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)
    wordCountDS.print()
  }
}
