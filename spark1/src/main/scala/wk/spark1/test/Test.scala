package wk.spark1.test

object Test {

  type DATA = (String,Int)

  def test1(): Unit = {
    def simplify(track:List[DATA]):List[DATA] = {
      def f:(DATA,List[DATA])=>List[DATA] = (p:DATA, track:List[DATA])=> track match {
        case List() => p::track
        case p2::track2 => if (p._2==p2._2) f(p,track2) else p::f(p2,track2)
      }
      f(track.head,track.tail)
    }
    val track = List(("liubei",1),("liubei2",1),("guanyu",2),("guanyu2",2),("zhangfei",3)).sortWith(_._2<_._2)
    val track2 = simplify(track)
    track2.foreach(println)
  }

  def test2() = {
    //val a = List(1,2,3)
    val a = List[Int]()
    val b = a.reduce((a,b)=>a+b)
    println(b)
  }

  def main(args: Array[String]): Unit = {
    test2()
  }


}
