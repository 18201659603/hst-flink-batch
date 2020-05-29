package org.hopson.hst.streaming

import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.hopson.hst.hbase.HBaseReader

object StreamingRichSourceHbase {
  /**
    * 从HBase读取数据
    * 第一种：继承RichSourceFunction重写父类方法
    */
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.enableCheckpointing(5000)
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

  val dataStram: DataStream[(String,String)] = env.addSource(new HBaseReader)
  dataStram.map(x => println(x._1 + " " + x._2))
  env.execute()
}
