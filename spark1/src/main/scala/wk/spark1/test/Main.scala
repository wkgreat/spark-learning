package wk.spark1.test

import org.apache.spark.sql.SparkSession

object Main {

  val ss = SparkSession.builder().appName("test").master("local[*]").getOrCreate()
  val sc = ss.sparkContext

  def main(args: Array[String]): Unit = {

    sc.setLogLevel("WARN")

  }

  def test0(): Unit = {

    var rdd = sc.parallelize(Seq("nanjing","beijing","shanghai","guangzhou"))
    rdd = sc.emptyRDD[String]
    rdd.reduce((x,y)=>x+y).foreach(println)
  }


  //groupby使用的key，如何判断key相等的，可以使用key的某个字段吗
  def test1(): Unit = {

    case class TheCase(id:Int, name:String) extends Comparable[TheCase]{

      override def hashCode(): Int = this.id % 1024

      override def equals(obj: Any): Boolean = {
        this.id == obj.asInstanceOf[TheCase].id
      }

      override def compareTo(o: TheCase): Int = this.id - o.id
    }
    val rdd = sc.parallelize(Seq(
      (TheCase(1, "aa"), "sdf"),
      (TheCase(2, "bb"), "sdfsdf"),
      (TheCase(1, "cc"), "ddssdd")
    ))

    val rdd2 = rdd.groupBy(_._1)
    rdd2.collect.foreach(println)

  }

}
