package com.unis.xjbigdata

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import util.control.Breaks._
object MyTest {
  def main(args: Array[String]): Unit = {
    val nowtime=System.currentTimeMillis();
    println(DateUtils.tranTimeToString(nowtime))
    val last=nowtime-100*24*60*60*1000L;
    println(DateUtils.tranTimeToString(last))


    println(DateUtils.tranTimeToLong("2018-10-12 14:55:22"))



    for(i<-0 until 10){
      breakable{
        if(i==3||i==6) {
          break
        }else{

          println(i)
        }
      }
      println(i+":::::")
    }

    val str1="0.000"
    str1.toDouble
    println(str1.toDouble==0)



    val temp="100001"+","+System.currentTimeMillis()
    val aa=temp.split(",")
    println(aa(0)+aa(1))
    val tmp= new ListBuffer[Int]
    val tmp2=ListBuffer(1,2,3)
    val tmp3=ListBuffer(4,5,6)
    val tmp4=tmp3++(tmp2)
    println(tmp4)
    val map1=new mutable.HashMap[String,String]()
    map1.put("a","1")
    println(map1)
    map1("a")="2"
    println(map1)
    println(tmp2.+:(89))
    val str="1537870080562"
    val sb=new mutable.StringBuilder(str).reverse.toString
    println(sb)
    println(DateUtils.tranTimeToLong("2018-09-27 21:26:10"))
    println(DateUtils.tranTimeToLong("2018-09-26 21:26:10"))
    println(DateUtils.tranTimeToString(1538041435000L))

    if(str.contains("1")){
      println("1")
    }else if(str.contains("5")){println("5")}


  }
}
