package wk.spark1

import scala.reflect.ClassTag

trait Example {
  def run():Unit
}

object LearnScala {

  def main(args: Array[String]): Unit = {
    E4.run()
  }

  /** wk 匿名函数，函数作为参数，闭包 */
  object E1 extends Example {

    def process[T,U:ClassTag](func:T=>U, seq: Seq[T]): Array[U] = {
      val results:Array[U] = new Array[U](seq.size) //wk 被闭包
      val callFunc: (Int,U)=>Unit = (index,res) => results(index) = res //wk 函数变量
      call(func,seq,callFunc) //wk 函数作为参数
      results
    }
    def call[T,U](func:T=>U, seq: Seq[T], handler:(Int,U)=>Unit): Unit = {
      seq.indices.foreach(i=>handler(i,func(seq(i))))
    }
    def run(): Unit = {
      val theSeq = Seq[Int](1,2,3,4,5)
      val func:Int=>String = i=>"#"+i
      val r = process[Int,String](func,theSeq)
      r foreach println
    }

  }

  /** wk _* 的用法 */
  object E2 extends Example {

    def func(a: Int*): Unit = {
      a.foreach(println)
    }

    def func2(): Unit = {

      val a = Array(1,2,3,4)
      val Array(x,y,_*) = a // _* usage2
      println(x+","+y)

    }

    def run(): Unit = {

      val paramArray = Array(1,2,3)
      func(paramArray: _*) // _* usage 1
      func2()
    }

  }

  /** 传名参数 */
  object E3 extends Example {

    var d = 0

    def func(i: => Int) = {
      println("i1:"+i)
      d = 5
      println("i2:"+i) //i是传名参数，每次调用i，i (d+5)都会重新计算一次
    }

    override def run(): Unit = {
      func(d+5) //d+5为传名参数
    }

  }

  /** 传名参数2 */
  object E4 extends Example {

    def func(calc:Boolean)(body: => Int) {
      println("func")
      if(calc) body
      println("func End")
    }

    override def run(): Unit = {
      func(false) {
        println("haha")
        1
      }
    }

  }


}

