package com.unis.xjbigdata

import java.io.IOException
import java.util

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object ToHbase {

   val conn=getHbaseConnection()
  def insertHbaseData(hbaseTableName: String, htmap: mutable.LinkedHashMap[String, ListBuffer[String]]): Unit = {
    val tableName = TableName.valueOf(Bytes.toBytes(hbaseTableName))
    val family=matchFamily(hbaseTableName)
    val puts = new util.ArrayList[Put]()
    for (ht <- htmap) {
      val tmpKey = ht._1.split(",")
      //可采用id+timestamp，通过startrow endrow测试一把
      val rowkey = tmpKey(0)+tmpKey(1)
      //val rowkey = tmpKey(1).reverse + tmpKey(0)
      val put = new Put(Bytes.toBytes(rowkey))
      put.addColumn(Bytes.toBytes(family), Bytes.toBytes("Id"), Bytes.toBytes(tmpKey(0)))
      put.addColumn(Bytes.toBytes(family), Bytes.toBytes("SaveTime"), Bytes.toBytes(DateUtils.tranTimeToString(tmpKey(1).toLong)))
      for (value <- ht._2) {
        val tmp = value.split(",")
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(tmp(1)), Bytes.toBytes(tmp(0)))
      }
      puts.add(put)
    }
    try {
      val table = conn.getTable(tableName)
      table.put(puts)
      table.close()
    } catch {
      case e: IOException => {
        e.printStackTrace()
      }
    }
  }
  def matchFamily(hbaseTableName: String): String = hbaseTableName match {
    case "xj_ht_data" => "Ht"
    case "xj_qdl_ycdata" => "QdlYc"
    case "xj_qdl_ymdata" => "QdlYm"
    case "xj_alarm_data" => "Alarm"
  }
  def getHbaseConnection(): Connection = {
    val conf = HBaseConfiguration.create()
    var conn: Connection = null;
    conf.set("hbase.zookeeper.quorum", "10.1.11.121:2181,10.1.11.122:2181,10.1.11.123:2181")
    conf.set("zookeeper.znode.parent", "/hbase-unsecure")
    //conf.set("hbase.master", "192.168.1.25:16000");
    //conf.set("hbase.zookeeper.property.clientPort", "2181");
    try {
        conn = ConnectionFactory.createConnection(conf)
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
    conn
  }

  def CloseHbaseConnection(conn: Connection): Unit = {
    try {
      if (!conn.isClosed || conn != null) {
        conn.close()
      }
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }
}
