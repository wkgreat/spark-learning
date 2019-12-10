package com.imooc.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Approach 1: Flume-style Push-based Approach
 * */
object FlumePushWordCount {

  def main(args: Array[String]): Unit = {

    if(args.length != 2) {
      System.err.println("FlumePushWordCount <hostname> <port>")
      System.exit(1)
    }

    val Array(hostname, port) = args

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("FlumePushWordCount")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    ssc.sparkContext.setLogLevel("ERROR")

    val flumeStream = FlumeUtils.createStream(ssc, hostname, port.toInt)

    flumeStream.map(e=>new String(e.event.getBody.array()).trim).flatMap(_.split(" "))
      .map((_,1)).reduceByKey(_+_).print()

    ssc.start()
    ssc.awaitTermination()


  }

}
