package com.unis.xjbigdata

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

object DateUtils {

  val calendar = Calendar.getInstance()
  def getDataTime():String={
    val date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val today = calendar.getTime()
    date.format(today)
  }
  //把日期转化为时间戳
  def apply(time: String) :Long= {
    val date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    var date2=date.parse(time)
    var result= date2.getTime
    result
  }

  def getCertainDayTime(amount: Int): Long ={
    calendar.add(Calendar.DATE, amount)
    val time = calendar.getTimeInMillis
    calendar.add(Calendar.DATE, -amount)
    time
  }
  //获取当天日期
  def getNowDate():String={
    var now:Date = new Date()
    var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    var hehe = dateFormat.format( now )
    hehe
  }
  //获取当年
  def getNowYear():String={
    var now:Date = new Date()
    var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy")
    var hehe = dateFormat.format( now )
    hehe
  }

  //获取昨天的日期
  def getYesterday():String={
    var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal:Calendar=Calendar.getInstance()
    cal.add(Calendar.DATE,-1)
    var yesterday=dateFormat.format(cal.getTime())
    yesterday
  }

  //获取本周开始日期
  def getNowWeekStart():String={
    var period:String=""
    var cal:Calendar =Calendar.getInstance();
    var df:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY)
    //获取本周一的日期
    period=df.format(cal.getTime())
    period
  }
  //获取本周末的时间
  def getNowWeekEnd():String={
    var period:String=""
    var cal:Calendar =Calendar.getInstance();
    var df:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    cal.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);//这种输出的是上个星期周日的日期，因为老外把周日当成第一天
    cal.add(Calendar.WEEK_OF_YEAR, 1)// 增加一个星期，才是我们中国人的本周日的日期
    period=df.format(cal.getTime())
    period
  }

  //本月的第一天
  def getNowMonthStart():String={
    var period:String=""
    var cal:Calendar =Calendar.getInstance();
    var df:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    cal.set(Calendar.DATE, 1)
    period=df.format(cal.getTime())//本月第一天
    period
  }
  //本月的最后一天
  def getNowMonthEnd():String={
    var period:String=""
    var cal:Calendar =Calendar.getInstance();
    var df:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    cal.set(Calendar.DATE, 1)
    cal.roll(Calendar.DATE,-1)
    period=df.format(cal.getTime())//本月最后一天
    period
  }
  //时间戳转化为时间
  def tranTimeToString(tm:Long) :String={
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val tim = fm.format(new Date(tm))
    tim
  }
  //时间转换为时间戳
  def tranTimeToLong(tm:String) :Long={
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dt = fm.parse(tm)
    val aa = fm.format(dt)
    val tim: Long = dt.getTime()
    tim
  }
  //获取昨天的开始日期
  def getStartTime() :String={
    val cur:Date = new Date()
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    import java.util.Calendar
    calendar.add(Calendar.DAY_OF_MONTH, -1)
    import java.util.Calendar
    calendar.set(Calendar.HOUR_OF_DAY, 0)
    calendar.set(Calendar.MINUTE, 0)
    calendar.set(Calendar.SECOND, 0)
    calendar.set(Calendar.MILLISECOND, 0)
    val dayStart = calendar.getTime
    val startStr = fm.format(dayStart)
    startStr
  }
  //获取昨天的结束日期
  def getEndTime() :String={
    val cur:Date = new Date()
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    import java.util.Calendar
    calendar.add(Calendar.DAY_OF_MONTH, 0)
    import java.util.Calendar
    calendar.set(Calendar.HOUR_OF_DAY, 23)
    calendar.set(Calendar.MINUTE, 59)
    calendar.set(Calendar.SECOND, 59)
    calendar.set(Calendar.MILLISECOND, 999)
    val dayEnd = calendar.getTime
    val endStr = fm.format(dayEnd)
    endStr
  }

}
