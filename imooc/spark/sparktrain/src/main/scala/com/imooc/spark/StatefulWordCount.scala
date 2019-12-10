package com.imooc.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
/**
 * 8-2 UpdateStateByKey 算子的使用
 * */
object StatefulWordCount {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("StatefulWordCount")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    ssc.sparkContext.setLogLevel("ERROR")

    // 在使用有状态的算子之前必须设置checkpoint。生产环境中配置为HDFS的某个文件夹
    ssc.checkpoint("./checkpoint")

    val lines = ssc.socketTextStream("localhost",6789)

    val result = lines.flatMap(_.split(" ")).map((_,1))

    /**
     * updateStateByKey使用到的状态更新函数
     * currentS 为 Seq[A]，即当前批次某个key的所有的值
     * preS: Option[S] 为之前的状态
     * 返回: Option[S] 为当前更新的状态
     * */
    //val stateFunc: (Seq[Int], Option[Int]) => Option[Int] = (currentS, preS) => Some(currentS.sum + preS.getOrElse(0))
    def stateFunc(currentS: Seq[Int], preS: Option[Int]) = Some(currentS.sum + preS.getOrElse(0))

    val state = result.updateStateByKey(stateFunc)

    state.print()

    ssc.start()

    ssc.awaitTermination()

  }

}
