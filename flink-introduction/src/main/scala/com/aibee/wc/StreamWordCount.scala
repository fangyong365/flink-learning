package com.aibee.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._


/**
  * @author shkstart
  * @create 2020-10-25 17:36
  */
object StreamWordCount {
  def main(args: Array[String]): Unit = {
    //从外部命令中获取参数
    val params: ParameterTool = ParameterTool.fromArgs(args)
    val host: String = params.get("host")
    val port: Int = params.getInt("port")

    //创建流处理环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //接收socket文本流
    val inputDataStream: DataStream[String] = env.socketTextStream(host,port)

    //flatMap和map需要隐式转化
    val resultDataStream: DataStream[(String, Int)] = inputDataStream
      .flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    resultDataStream.print()

    //启动任务执行
    env.execute()

  }
}
