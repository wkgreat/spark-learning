package com.imooc.spark

import java.sql.{Connection, DriverManager}

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 8-3 将统计结果写入MySQL
 * */
object ForeachRDDApp {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("ForeachRDDApp")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    ssc.checkpoint("./checkpoint")

    val lines = ssc.socketTextStream("localhost", 6789)

    val result = lines.flatMap(_.split(" ")).map((_,1))

    val state = result.updateStateByKey((cS: Seq[Int], pS: Option[Int])=>Some(cS.sum+pS.getOrElse(0)))

    state.foreachRDD(rdd=>{
      rdd.foreachPartition(iter=>iter.foreach(t=>{
        val ct = getConnnection()
        val sql = s"insert into word_count values ('${t._1}',${t._2})"
        val st = ct.createStatement()
        st.execute(sql)
      }))
    })

    ssc.start()
    ssc.awaitTermination()

    def getConnnection(): Connection = {

      Class.forName("com.mysql.jdbc.Driver")
      DriverManager.getConnection("jdbc:mysql://localhost:3306/imooc_spark", "root", "12345")

    }

  }

}
