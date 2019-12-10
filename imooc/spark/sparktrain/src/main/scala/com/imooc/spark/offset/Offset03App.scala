package com.imooc.spark.offset

import _root_.kafka.serializer.StringDecoder
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Offset03App {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("Offset03App").setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf, Seconds(10))

    ssc.sparkContext.setLogLevel("ERROR")

    val kafkaParams = Map[String,String](
      "metadata.broker.list" -> "hadoop000:9092",
      "auto.offset.reset" -> "smallest"
    )
    val topics = Set[String]("imooc_pk_offset")


    val fromOffsets = Map[TopicAndPartition, Long]()
    // TODO: 获取偏移量...

    val messages = if(fromOffsets.isEmpty) { //从头消费
      KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topics)
    } else { //从指定偏移量开始消费
      val messageHandler = (mm:MessageAndMetadata[String,String]) => (mm.key(), mm.message())
      KafkaUtils.createDirectStream[
        String,String,StringDecoder,StringDecoder,(String,String)
      ](ssc,kafkaParams,fromOffsets, messageHandler)
    }

    messages.transform(rdd=>{ //offset提交
      val offsetRange = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      offsetRange.foreach(x=>{/*TODO: 提交信息到ZK...*/})
      rdd
    }).foreachRDD(rdd=>{ //业务
      println("慕课PK哥:" + rdd.count())
    })

    ssc.start()
    ssc.awaitTermination()

  }

}
