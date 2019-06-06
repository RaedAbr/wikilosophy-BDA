import org.apache.spark.graphx.Graph

object shortestPath {
  def run(graph: Graph[WikiArticle, Double], sourceId: Long) ={
    val g = graph.mapVertices { (id, _) =>
      if (id == sourceId) (List[Long](), 0.0) else (List[Long](), Double.PositiveInfinity)
    }
    val sssp = g.pregel(
      initialMsg = (List[Long](), Double.PositiveInfinity)
    )(
      (_, tuple, newTuple) => { //vprog -> process message
        if (tuple._2 < newTuple._2) tuple
        else newTuple
      },
      triplet => { //sendMsg decide within neighbors if message is required
        if (triplet.srcAttr._2 + triplet.attr < triplet.dstAttr._2) {
          Iterator((triplet.dstId, (triplet.srcId::triplet.srcAttr._1, triplet.srcAttr._2 + triplet.attr)))
        }
        else {
          Iterator.empty
        }
      },
      (a, b) => { //Message aggregator
        if (a._2 < b._2) a else b //choose the one with smallest path
      }
    )
    sssp.outerJoinVertices(graph.vertices){
      (id, left, rightOpt) => (rightOpt.getOrElse(WikiArticle("SparkError")), left._1.reverse, left._2)
    }
  }
}
