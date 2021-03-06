package com.unis.xjbigdata

import java.sql.SQLException
import com.unis.javautil.{DataBaseConfig, JdbcUtil, PropertiesUtil}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object xjmain {
  private val logger = LoggerFactory.getLogger(xjmain.getClass)

  def main(args: Array[String]): Unit = {
    val qyid = getAllId(QuerySql.qdl_qy_sql)
    val ycmap = getAllCdMap("遥测", qyid)
    insertIntoHbase(QuerySql.qdl_ycTable, "遥测", ycmap)
//    val ymmap = getAllCdMap("遥脉", qyid)
//    insertIntoHbase(QuerySql.qdl_ymTable, "遥脉", ymmap)
//    val ht_qyid = getAllId(QuerySql.ht_qy_sql)
//    //注意多个火探共用一个温度，所以savetime相同，但datetime不同，因为不同设备采集时间不同
//     val htmap = getAllCdMap("遥测", ht_qyid)
//    insertIntoHbase(QuerySql.ht_ycTable, "遥测", htmap)
//    val alarmMap=getOneDayAlarmData()
//    ToHbase.insertHbaseData(QuerySql.alarmTable, alarmMap)
//    ToHbase.CloseHbaseConnection(ToHbase.conn)
  }

  /**
    * 批量入库Hbase
    *
    * @param hbaseTableName hbase表名
    * @param c_type         测点类型
    * @param ycmap          测点map
    */

  def insertIntoHbase(hbaseTableName: String, c_type: String, ycmap: mutable.LinkedHashMap[String, ListBuffer[String]]): Unit = {
    for (map <- ycmap) {
      val qyid = map._1
      val yclist = map._2
      //println("按顺序排列得测点id："+yclist)
      val tmpData = new mutable.LinkedHashMap[String, ListBuffer[String]]
      for (ycid <- yclist) {
        //一个区域一天得数据
        //getOneDayQyData(tmpData, c_type, qyid, ycid, JdbcUtil.getProp.getString("dataTime"))
        //一个区域一分钟的数据
        getOneDayQyData(tmpData, c_type, qyid, ycid, JdbcUtil.getProp.getString("st"), JdbcUtil.getProp.getString("et"))
      }
      //没查到数据输出日志，查到数据则清理数据为0.000（许继系统初始化数据）的kv
      if (tmpData.size == 0) {
        if ("xj_qdl_ymdata".equals(hbaseTableName)){
          //遥脉数据数每小时的整点有1条，此处一分钟此次肯定为0，故不输出日志
        }else{
          logger.error(s"表名为:${hbaseTableName},区域id为:${qyid},没有查到数据，数据量为：${tmpData.size}")
        }
      } else {
        for (map <- tmpData) {
          if (map._2.size == 1) tmpData.remove(map._1)
        }
      }
      if (tmpData.size != 0) {
        //ToHbase.insertHbaseData(hbaseTableName,tmpData)
        if ("xj_qdl_ymdata".equals(hbaseTableName)) {
          //遥脉数据正常一小时1条，不正常则每分钟都有
          if (tmpData.size > 1) {
            logger.error(s"表名为:${hbaseTableName},区域id为:${qyid},整点入库数据量为：${tmpData.size}")
          }
        } else if (tmpData.size != 12) {
          //正常则每分钟12条，不正常则<12
          logger.error(s"表名为:${hbaseTableName},区域id为:${qyid},每分入库数据量为：${tmpData.size}")
        }
      }
      //输出一个设备每个时间点的所有数据
      tmpData.foreach(f => println(f._1 + "," + f._2.toString()))
      //logger.info(s"表名为:${hbaseTableName},区域id为:${qyid},查询的数据量为：${tmpData.size}")
    }
  }

  /**
    * 获取一个区域一天的数据
    *
    * @param tmpMap 临时数据集聚map
    * @param c_type 测点类型
    * @param qyid   区域id
    * @param ycid   测点id
    *               //@param ymdTime 年月日
    * @param st     开始时间
    * @param et     结束时间
    * @return
    */

  def getOneDayQyData(tmpMap: mutable.LinkedHashMap[String, ListBuffer[String]], c_type: String, qyid: String, ycid: String, st: String, et: String): mutable.LinkedHashMap[String, ListBuffer[String]] = {
    val year = DateUtils.getNowYear()
    val id = ycid.split(",")(0)
    val c_code = ycid.split(",")(1)
    val mode = id.toInt % 10
    var tableName = ""
    var cdsql = ""
    if ("遥测".equals(c_type)) {
      tableName = s"hisanalog_${year}_0${mode}"
      //一天抽取一次
      //cdsql = s"SELECT savetime,curdatatime,calvalue FROM ${tableName} WHERE analogid=${id} and DATE_FORMAT(savetime,'%Y-%m-%d')='${ymdTime}' "
      //一分钟抽取一次
      cdsql = s"SELECT savetime,curdatatime,calvalue FROM ${tableName} WHERE analogid=${id} and savetime>='${st}' and savetime<'${et}' "
    } else {
      tableName = s"hisaccumulator_${year}_0${mode}"
      //cdsql = s"SELECT savetime,datetime,curvalue FROM ${tableName} WHERE accumulatorid=${id} and DATE_FORMAT(savetime,'%Y-%m-%d')='${ymdTime}' "
      cdsql = s"SELECT savetime,datetime,curvalue FROM ${tableName} WHERE accumulatorid=${id} and savetime>='${st}' and savetime<'${et}' "
    }
    try {
      val conn = JdbcUtil.getConnection
      val state = conn.createStatement()
      val rs = state.executeQuery(cdsql)
      while (rs.next()) {
        //数据key 为id+savetime
        //value 为calvalue

        val key = qyid + "," + rs.getTimestamp(1).getTime
        val value = rs.getString(3) + "," + c_code

        if ("I".equals(c_code) || "Ua".equals(c_code) || "WP".equals(c_code)) {
          val valueT = DateUtils.tranTimeToString(rs.getTimestamp(2).getTime) + "," + "DateTime"
          if (tmpMap.contains(key)) {
            //此处为判断火探电流和温度的先后顺序，而全电量都是有序的，如果是ht数据，第一次是T，第二次
            //则为I，把数据放到list前面
            val list = tmpMap(key)
            if (rs.getDouble(3) != 0) {
              tmpMap(key) = list.+:(value).+:(valueT)
            } else {
              tmpMap(key) = list.+:(valueT)
            }

          } else {
            //第一次把数据放到list前面
            //判断value为0则过滤
            if (rs.getDouble(3) != 0) {
              tmpMap.put(key, ListBuffer[String](valueT, value))
            } else {
              tmpMap.put(key, ListBuffer[String](valueT))
            }

          }
        } else {
          if (tmpMap.contains(key)) {
            //入电流，电流一次封装两个数据，形成的新的listbuffer为datetime,I,T
            //入温度或全电量，一次封装一个数据,全电量为Ua、Ub.... WP、WPJ....
            val list = tmpMap(key)
            if (rs.getDouble(3) != 0) {
              tmpMap(key) = list.:+(value)
            }
          } else {
            //第一次为T，则走下面一步
            if (rs.getDouble(3) != 0) tmpMap.put(key, ListBuffer[String](value))
          }
        }
      }
      JdbcUtil.close(state)
      JdbcUtil.close(rs)
    } catch {
      case e1: SQLException => logger.error("sqlException" + e1.getMessage)
      case e2: Exception => logger.error("其他异常" + e2.getMessage)
    } finally {
      JdbcUtil.closeConnection()
    }
    tmpMap
  }

  /**
    * 获取所有全电量设备（区域）的遥测id,key实际为许继设备id，值为yc集合
    * 获取所有全电量设备（区域）的遥信id,key实际为许继设备id，值为ym集合
    * 获取所有火探的遥测id,key实际为许继漏电流测点id，值为yc（漏电流id与温度id）集合
    *
    * @param c_type 测点类型
    * @param qylist 区域id集合
    * @return
    */
  def getAllCdMap(c_type: String, qylist: ListBuffer[String]): mutable.LinkedHashMap[String, ListBuffer[String]] = {
    val ycmap = new mutable.LinkedHashMap[String, ListBuffer[String]]()
    for (qyid <- qylist) {
      val ycsql = s"select c_id,c_code from xj_cd_big where q_id= ${qyid} and c_type='${c_type}' order by c_id"
      val ycID = getAllId(ycsql)
      // println("区域id："+qyid+" cd:"+ycID.mkString(","))
      ycmap.+=((qyid, ycID))
    }
    ycmap
  }

  /**
    * 获取所有id
    *
    * @param sql 查询
    * @return id集合
    */
    def getAllId(sql: String): ListBuffer[String] = {
    val idlist = new ListBuffer[String]

    try {
      val conn = JdbcUtil.getConnection
      val state = conn.createStatement()
      val rs = state.executeQuery(sql)
      val colnum = rs.getMetaData.getColumnCount
      while (rs.next()) {
        if (colnum > 1) {
          //查询测点id,c_code
          val sb = new mutable.StringBuilder().append(rs.getInt(1)).append(",").append(rs.getString(2)).toString()
          idlist.+=(sb)
        } else {
          //查询区域id
          idlist.+=(rs.getString(1))
        }
      }
      JdbcUtil.close(state)
      JdbcUtil.close(rs)
    } catch {
      case e1: SQLException => logger.error("sqlException:" + e1.getMessage)
      case e2: Exception => logger.error("其他异常:" + e2.getMessage)
    } finally {
      JdbcUtil.closeConnection()
    }
    idlist
  }


  /**
    * 获取数据库配置
    *
    * @return
    */
  def getDataBaseConfig(): DataBaseConfig = {
    val dataBaseConfig = new DataBaseConfig()
    val propertiesUtil = new PropertiesUtil("jdbc.properties")
    dataBaseConfig.setDriverClass(propertiesUtil.getString("jdbc.driverClass"))
      .setUserName(propertiesUtil.getString("jdbc.username"))
      .setPassWord("jdbc.password")
      .setUrl("jdbc.url").getDataBaseConfig
  }

  /**
    * 查询告警数据
    * @return
    */
  def getOneDayAlarmData(): mutable.LinkedHashMap[String, ListBuffer[String]] = {

    val tmpMap = new mutable.LinkedHashMap[String, ListBuffer[String]]
    try {
      val st = JdbcUtil.getProp.getString("st")
      val et = JdbcUtil.getProp.getString("et")
      val alarm_sql=s"SELECT sid ,savetime ,Descriptions ,Positions  FROM (SELECT objid,objtype,thetime AS SaveTime ,description AS Descriptions FROM hisalarm WHERE thetime>'${st}' AND thetime<='${et}' AND objtype IN('soe','analog')) his INNER JOIN (SELECT s_id AS Sid,S_position AS Positions FROM xj_sb_big) sb ON his.objid=sb.Sid"
      val conn = JdbcUtil.getConnection
      val state = conn.createStatement()
      val rs = state.executeQuery(alarm_sql)
      val Rsmd = rs.getMetaData
      while (rs.next()) {
        //数据key 为sid+savetime
        //value 为desc+position
        val key = rs.getString(1) + "," + rs.getTimestamp(2).getTime
        val desc = rs.getString(3).trim + "," + Rsmd.getColumnName(3)
        val position = rs.getString(4).trim + "," + Rsmd.getColumnName(4)
        val listBuffer = ListBuffer[String](desc, position)
        tmpMap.put(key, listBuffer)
      }
      JdbcUtil.close(state)
      JdbcUtil.close(rs)
    } catch {
      case e1: SQLException => logger.error("sqlException" + e1.printStackTrace())
      case e2: Exception => logger.error("其他异常" + e2.printStackTrace())
    } finally {
      JdbcUtil.closeConnection()
    }
    tmpMap
  }


}

