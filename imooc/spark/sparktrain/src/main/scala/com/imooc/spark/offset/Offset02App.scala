package com.imooc.spark.offset

import _root_.kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

object Offset02App {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("Offset02App").setMaster("local[2]")

    val kafkaParams = Map[String,String](
      "metadata.broker.list" -> "hadoop000:9092",
      "auto.offset.reset" -> "smallest"
    )
    val topics = Set[String]("imooc_pk_offset")

    val checkpointDirectory = "hdfs://hadoop000:8020/offset-kafka"
    def functionToCreateContext(): StreamingContext = {
      val ssc = new StreamingContext(sparkConf, Seconds(10))
      val messages =
        KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topics)
      ssc.checkpoint(checkpointDirectory) //设置checkpoint目录
      messages.checkpoint(Duration(10*1000)) //定时checkpoint
      messages.foreachRDD(rdd=>{println("慕课PK哥:" + rdd.count())}) //业务
      ssc
    }
    val ssc = StreamingContext.getOrCreate(checkpointDirectory, functionToCreateContext _)
    ssc.start()
    ssc.awaitTermination()



  }

}
