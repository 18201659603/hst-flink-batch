package org.hopson.hst.batch

import org.apache.flink.api.scala.ExecutionEnvironment
import org.hopson.hst.hbase.HBaseInputFormat
import org.hopson.hst.streaming.StreamingRichSourceHbase.{dataStram, env}

object BatchReadHBase {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    /**
      * 读取HBase数据方式：实现TableInputFormat接口
      */
    val dataStream = env.createInput(new HBaseInputFormat)

    dataStram.map(x => println(x._1 + " " + x._2))
    env.execute()
  }


}
