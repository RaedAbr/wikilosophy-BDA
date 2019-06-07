import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession

class VertexProperty()
case class WikiArticle(title: String) extends VertexProperty

object wikiGraphReader {
  def parseWikiGraph(spark: SparkSession): Graph[WikiArticle, Double] = {
    val s3 = "s3a://wikilosophy-data/"
    val data = "data/"

    val simpleWikiVertices = "simpleWikiIdWithString.txt"
    val simpleWikiEdges = "simpleWikiEdgesWithId.txt"

    val wikiVertices = "idWithString.txt"
    val wikiEdges = "edgesWithId.txt"

    val verticesRDD = spark.sparkContext.textFile(data+simpleWikiVertices)
      .map(line => {
        val splitLine = line.split("\t").toList
        val id = splitLine(0).toLong
        val title = splitLine(1)
        (id, WikiArticle(title))
      })

//      .zipWithIndex()
//      .map(x => x match { case (title: String, id: Long) => (id+1.toLong, WikiArticle(title.replace("_"," ")))})

    val edgesRDD = spark.sparkContext.textFile(data+simpleWikiEdges).flatMap(line => {
      val split = line.split(": ").toList
      val id = split.head.toLong
      split.tail.head.split(" ").toList.map(link => Edge(id, link.toLong , 1.0))
    })


    val defaultArticle = WikiArticle("Wrong article")
    Graph(verticesRDD, edgesRDD)
//    Graph(null, null)
  }
}
