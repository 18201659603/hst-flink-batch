package org.hopson.hst.streaming

import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.hopson.hst.hbase.HBaseInputFormat
/**
  * 从HBase读取数据
  * 第二种：实现TableInputFormat接口
  */
object StreamingTableInputHBase {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.enableCheckpointing(5000)
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

  val dataStream = env.createInput(new HBaseInputFormat)
  dataStream.filter(_.f0.startsWith("10")).print()
  env.execute()
}
