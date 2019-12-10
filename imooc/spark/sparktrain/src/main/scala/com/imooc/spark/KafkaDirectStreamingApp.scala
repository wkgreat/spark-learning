package com.imooc.spark

import _root_.kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaDirectStreamingApp {

  def main(args: Array[String]): Unit = {

    if(args.length != 2) {
      System.err.println("Usage: KafkaDirectWordCount brokers, topics")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("KafkaDirectWordCount")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    ssc.sparkContext.setLogLevel("ERROR")

    val Array(brokers,topics) = args
    val kafkaParams = Map[String,String]("metadata.broker.list"->brokers)
    val topicsSet = topics.split(",").toSet
    val kafkaStream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](
      ssc, kafkaParams, topicsSet
    )

    //kafkaStream.map(_._2).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()
    var offsetRanges = Array.empty[OffsetRange]
    val stream = kafkaStream.transform(rdd=>{
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }).map(_._2)
//    stream.foreachRDD(rdd=>{
//      for(o <- offsetRanges) {
//        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
//      }
//    })
    stream.count().print()

    ssc.start()
    ssc.awaitTermination()

  }

}
