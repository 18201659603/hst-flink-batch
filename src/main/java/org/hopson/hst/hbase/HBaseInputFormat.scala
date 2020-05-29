package org.hopson.hst.hbase

import java.io.IOException

import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.addons.hbase.TableInputFormat
import org.apache.flink.configuration.Configuration
import org.apache.hadoop.hbase.{Cell, HBaseConfiguration, HConstants, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConverters._
/**
  *
  * 从HBase读取数据
  * 第二种：实现TableInputFormat接口
  */
class HBaseInputFormat extends CustomTableInputFormat[Tuple2[String, String]]{

  // 结果Tuple
  val tuple2 = new Tuple2[String, String]
  //zk的地址
  val zookeeper = "dp-hadoop-6,dp-hadoop-7,dp-hadoop-8"
  //zk端口号
  val zookeeper_port = "2181"
  //表
  val hTable = "hopson:test"
  /**
    * 建立HBase连接
    * @param parameters
    */
  override def configure(parameters: Configuration): Unit = {
    val tableName: TableName = TableName.valueOf(hTable)
    val cf1 = "info"
    var conn: Connection = null
    val config: org.apache.hadoop.conf.Configuration = HBaseConfiguration.create

    config.set(HConstants.ZOOKEEPER_QUORUM, zookeeper)
    config.set(HConstants.ZOOKEEPER_CLIENT_PORT, zookeeper_port)
    config.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 30000)
    config.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 30000)

    try {
      conn = ConnectionFactory.createConnection(config)
      table = conn.getTable(tableName).asInstanceOf[HTable]
      scan = new Scan()
      scan.withStartRow(Bytes.toBytes("11"))
      scan.withStopRow(Bytes.toBytes("11"))
      scan.addFamily(Bytes.toBytes(cf1))
    } catch {
      case e: IOException =>
        e.printStackTrace()
    }
  }
  /**
    * 对获取的数据进行加工处理
    * @param result
    * @return
    */
  override def mapResultToTuple(result: Result): Tuple2[String, String] = {
    val rowKey = Bytes.toString(result.getRow)
    val sb = new StringBuffer()
    for (cell: Cell <- result.listCells().asScala){
      val value = Bytes.toString(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
      sb.append(value).append("_")
    }
    val value = sb.replace(sb.length() - 1, sb.length(), "").toString
    tuple2.setField(rowKey, 0)
    tuple2.setField(value, 1)
    tuple2
  }
    /**
      * tableName
      * @return
      */
    override def getTableName: String = hTable
    /**
      * 获取Scan
      * @return
      */
    override def getScanner: Scan = {
      scan
    }

}
