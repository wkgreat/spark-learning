package com.imooc.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 7-7 Spark Streaming 处理文件系统数据 (HDFS 或 本地文件)
 * */
object FileWordCount {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("FileWordCount")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val lines = ssc.textFileStream("file:///Users/wkgreat/hadoop/data/streaming")

    val result = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    result.print()

    ssc.start()

    ssc.awaitTermination()

  }

}
