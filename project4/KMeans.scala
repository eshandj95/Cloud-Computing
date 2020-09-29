import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object KMeans {

  type Point=(Double,Double)
  var centroids: Array[Point] = Array[Point]()



  def distance (p: Point, p1: Point): Double ={

    var d1 = Math.sqrt(Math.pow(Math.abs(p._1-p1._1), 2)+Math.pow(Math.abs(p._2-p1._2), 2));
    d1

  }

  def main (args: Array[String]): Unit ={

    val conf = new SparkConf().setAppName("KMeans")
    val sc = new SparkContext(conf)

    centroids = sc.textFile(args(1)).map( line => { val a = line.split(",")
      (a(0).toDouble,a(1).toDouble)}).collect
    var points=sc.textFile(args(0)).map(line=>{val b=line.split(",")
      (b(0).toDouble,b(1).toDouble)})

    for(i<- 1 to 5){
      val cs = sc.broadcast(centroids)
      centroids = points.map { p => (cs.value.minBy(distance(p,_)), p) }
        .groupByKey().map { case(e,k)=>
        var c=0
        var s1=0.0
        var s2=0.0

        for(n <- k) {
           c += 1
           s1+=n._1
           s2+=n._2
        }
        var c1=s1/c
        var c2=s2/c
        (c1,c2)

      }.collect
    }

centroids.foreach(println)
    }
}