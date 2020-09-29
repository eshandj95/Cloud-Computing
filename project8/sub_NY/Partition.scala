import org.apache.spark.graphx.{Graph => Graph1, VertexId}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.io._
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object Partition {
def main(args:Array[String])
{
 val conf = new SparkConf().setAppName("Eshan")
 val sc = new SparkContext(conf)
 var cnt = -1.toLong
 val edges: RDD[Edge[Long]] = sc.textFile(args(0)).map( line => { val (node, neighbours) = line.split(",").splitAt(1)
 												(node(0).toLong,neighbours.toList.map(_.toLong)) } )
												.flatMap( x => x._2.map(y => (x._1, y)))
												.map(nodes => Edge(nodes._1, nodes._2, nodes._1))
val g_test : Graph1[Long,Long] = Graph1.fromEdges(edges, "defaultProperty") .mapVertices(
          (id, _) => { cnt = cnt+1
          if(cnt<5) id else -1.toLong
        })

      val t2 = g_test.pregel((-1).toLong, 6)(
        (id, grp, newgrp) => math.max(grp, newgrp).toLong,
        triplet => {
          if (triplet.dstAttr == (-1).toLong) {
            Iterator((triplet.dstId, triplet.srcAttr ))
          } else {
            Iterator.empty
          }
        },
        (a, b) => math.max(a, b)
      )

  val f_out = t2.vertices.map(x => (x._2, 1)).reduceByKey(_+_).sortByKey().map(g => g._1.toString + " " +g._2.toString )
  f_out.collect().foreach(println)
}
}
