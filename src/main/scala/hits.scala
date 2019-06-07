import org.apache.spark.graphx.{Graph, VertexRDD}

import scala.annotation.tailrec

object hits {
  case class Scores(hubScore: Double, authScore: Double)

  def run(g: Graph[WikiArticle, Double], maxIter: Int): Graph[(WikiArticle, Scores), Double] ={
    def reducer(a: Scores, b:Scores):Scores = Scores(a.hubScore+b.hubScore, a.authScore + b.authScore)
    @tailrec
    def loop(g: Graph[Scores, Double], iter: Int): Graph[Scores, Double] ={
      iter match {
        case 0 => g
        case _ => {
          val msgs: VertexRDD[Scores] = g.aggregateMessages(
            ctx => {
              ctx.sendToDst(Scores(0.0, ctx.srcAttr.hubScore)) //send hub score to nodes pointed by current node
              ctx.sendToSrc(Scores(ctx.srcAttr.authScore, 0.0)) //send auth score to nodes pointing to current node
            }, reducer)
          val gUpdated = g.outerJoinVertices(msgs){
            (id, left, rightOpt) => {
              reducer(left, rightOpt.getOrElse(Scores(0, 0)))
            }
          }
          val hubScoreRootSquaredSum = Math.sqrt(gUpdated.vertices.collect().toList.map(x => x._2.hubScore*x._2.hubScore).reduce((a, b) => a+b))
          val authScoreRootSquaredSum = Math.sqrt(gUpdated.vertices.collect().toList.map(x => x._2.authScore*x._2.authScore).reduce((a, b) => a+b))
          val normalizedScoresG = gUpdated.mapVertices((_, scores) => {
            val normHubScore = scores.hubScore/hubScoreRootSquaredSum
            val normAuthScore = scores.authScore/authScoreRootSquaredSum
            Scores(normHubScore, normAuthScore)
          })
          loop(normalizedScoresG, iter-1)
        }
      }
    }
    val gInit = g.mapVertices((_, _) => Scores(1,1))
    val scoreG = loop(gInit, maxIter)
    g.outerJoinVertices(scoreG.vertices){
      (_, left, rightOpt) => {
        (left, rightOpt.getOrElse(Scores(0,0)))
      }
    }
  }
}
