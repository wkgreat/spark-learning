package com.imooc.spark.project

import com.imooc.spark.project.dao.{CourseClickCountDAO, CourseSearchClickCountDAO}
import com.imooc.spark.project.domain.{ClickLog, CourseClickCount, CourseSearchClickCount}
import com.imooc.spark.project.utils.DateUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

/**
 * 使用Spark Streaming处理Kafka过来的数据
 * 功能1：到今天为止实战课程的访问量
 * 功能2：从搜索引擎引流过来的访问数量
 * */
object ImoocStatStreamingApp {
  def main(args: Array[String]): Unit = {

    if(args.length!=4) {
      System.err.println("Usage: ImoocStatStreamingApp <zkQuorum>, <groupId>, <topicMap>, <numThreads>")
      System.exit(1)
    }

    val Array(zkQuorum, groupId, topics, numThreads) = args

    val topicMap = topics.split(",").map((_,numThreads.toInt)).toMap

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("ImoocStatStreamingApp")

    val ssc = new StreamingContext(sparkConf, Seconds(60))

    ssc.sparkContext.setLogLevel("ERROR")

    val messages = KafkaUtils.createStream(ssc, zkQuorum, groupId, topicMap)

    //messages.map(_._2).count().print

    val logs = messages.map(_._2)
    val cleanData = logs.map(line=> {
      val infos = line.split("\t")
      val url = infos(2).split(" ")(1)
      var courseId = 0
      if(url.startsWith("/class")) {
        val courseIdHTML = url.split("/")(2)
        courseId = courseIdHTML.substring(0, courseIdHTML.lastIndexOf(".")).toInt
      }
      ClickLog(infos(0), DateUtils.parseToMinute(infos(1)), courseId, infos(3).toInt, infos(4))
    }).filter(clicklog=>clicklog.courseId!=0)

    //cleanData.print()

    cleanData.map(x=>{
      (x.time.substring(0,8)+"_"+x.courseId,1)
    }).reduceByKey(_+_).foreachRDD(rdd=>{
      rdd.foreachPartition(partitionRecords=>{
        val list = partitionRecords.map(r=>CourseClickCount(r._1,r._2)).to[ListBuffer]
        CourseClickCountDAO.save(list)
      })
    })

    //功能2：从搜索引擎引流过来的点击数
    cleanData.map(x=>{
      val referer = x.referer.replaceAll("//","/")
      val splits = referer.split("/")
      var host = ""
      if(splits.length > 2) {
        host = splits(1)
      }
      (host, x.courseId, x.time)
    }).filter(_._1 != "").map(x=>{
      (x._3.substring(0,8) + "_" + x._1+ "_" + x._2, 1)
    }).reduceByKey(_+_).foreachRDD(rdd=>{
      rdd.foreachPartition(partitionRecords=>{
        val list = partitionRecords.map(r=>CourseSearchClickCount(r._1,r._2)).to[ListBuffer]
        CourseSearchClickCountDAO.save(list)
      })
    })

    ssc.start()
    ssc.awaitTermination()

  }
}
