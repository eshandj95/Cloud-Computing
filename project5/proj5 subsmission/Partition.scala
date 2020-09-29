import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Partition {

  val depth = 6
  var n1 = 1
  val m:Long = -1
  

  def part_1(vert:(Long,Long,List[Long])):List[(Long,Either[(Long,List[Long]),Long])]={
      var px = List[(Long,Either[(Long,List[Long]),Long])]()//((vert._1,Left(vert._2,vert._3)))
      if(vert._2>0) {vert._3.foreach(dx => {px = (dx,Right(vert._2))::px})}
      px = (vert._1,Left(vert._2,vert._3))::px
    px
  }

  def part_2(str:Array[String]): (Long,Long,List[Long]) ={
    var c1:Long = -1
    if(n1<=10){
      n1 = n1+1
      c1 = str(0).toLong
    }
    (str(0).toLong,c1,str.tail.map(_.toString.toLong).toList)
  }



  def part_3(v: (Long,Iterable[(Either[(Long,List[Long]),Long])])):(Long,Long,List[Long])={
    var adjacent:List[Long] = List[Long]()
    var cluster:Long = -1
    for (v1 <- v._2){
      v1 match{
        case Right(c) => {cluster=c}
        case Left((c,adj)) if (c>0)=> return (v._1,c,adj)
        case Left((m,adj)) => adjacent=adj
      }
    }
    return (v._1,cluster,adjacent)
  }

  def main ( args: Array[ String ] ) :Unit = {
    val conf = new SparkConf().setAppName("Partition")
    val sc = new SparkContext(conf)
    var g = sc.textFile(args(0)).map(line => part_2(line.split(",")))
    for (i <- 1 to depth){
      g = g.flatMap(vertex => part_1(vertex)).groupByKey().map(vertex => part_3(vertex))
    }
    var r1 = g.map(h => (h._2,1))
    var r2 = r1.reduceByKey(_+_).collect()
    r2.foreach(println)
  }
}