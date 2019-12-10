package wk.spark1.learning.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamExample {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("stream-example").setMaster("local[4]")
    val ssc = new StreamingContext(conf,Seconds(5))
    ssc.sparkContext.setLogLevel("error")
    val lines = ssc.socketTextStream("localhost",7777)
    val errorLines = lines.filter(_.contains("error"))
    errorLines.print()

    ssc.start()
    ssc.awaitTermination()

  }

}
