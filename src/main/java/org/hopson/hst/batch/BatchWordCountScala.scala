package org.hopson.hst.batch

import org.apache.flink.api.scala.ExecutionEnvironment


object BatchWordCountScala {

  def main(args: Array[String]): Unit = {

    val inputPath = "D:\\app\\test\\file"
    val outPut = "D:\\app\\test\\result2"


    val env = ExecutionEnvironment.getExecutionEnvironment
    val text = env.readTextFile(inputPath)

    //引入隐式转换
  import org.apache.flink.api.scala._

    val counts = text.flatMap(_.toLowerCase.split(" "))
      .filter(_.nonEmpty)
      .map((_,1))
      .groupBy(0)
      .sum(1)
    counts.writeAsCsv(outPut,"\n"," ").setParallelism(1)
    env.execute("batch word count")

  }
}
