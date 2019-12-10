package com.imooc.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaReceiverWordCount {

  def main(args: Array[String]): Unit = {

    if(args.length != 4) {
      System.err.println("Usage: KafkaReceiverWordCount zkQuorum, group, topics, numTreads")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("KafkaReceiverWordCount")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    ssc.sparkContext.setLogLevel("ERROR")

    val Array(zkQuorum, group, topics, numTreads) = args
    val topicMap = topics.split(",").map((_,numTreads.toInt)).toMap
    val kafkaStream = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap)

    kafkaStream.map(_._2).flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()

    ssc.start()
    ssc.awaitTermination()

  }

}
