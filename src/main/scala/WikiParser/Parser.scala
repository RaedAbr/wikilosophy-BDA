package WikiParser

import java.io.{ByteArrayInputStream, File, FileInputStream, FileWriter, InputStream, PrintWriter, SequenceInputStream}
import java.nio.file.{Files, Path, Paths}
import java.util
import java.util.{Collections, Comparator}

import javax.xml.transform.stream.StreamSource
import org.apache.commons.io.FileUtils

import scala.io.Source
import org.apache.spark.sql.SparkSession

import scala.collection
import scala.collection.mutable
import scala.util.Try
import scala.xml.pull.{EvElemEnd, EvElemStart, EvText, XMLEventReader}
import sys.process._

object Parser extends App{
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("WikiParser")
    .getOrCreate()

  //Path constants
  val tempPath = "data\\temp.xml.bz2"
  val tempPathUnzip = "data\\temp"
  val indexPath = "data\\enwiki-20190101-pages-articles-multistream-index.txt.bz2"
  val dataPath = "data\\enwiki-20190101-pages-articles-multistream.xml.bz2"
  val outputPath = "data\\output"

  //Regex for wikipedia hyperlinks in xml
  val hyperLinkRegex = "\\[\\[:?([^\\]|]+)(?:\\|((?:]?[^\\]|])*+))*\\]\\]([^\\[]*)"

  val indexZip = spark.sparkContext.textFile(indexPath)
  val offsets : Array[String] = indexZip.map(line => line.split(":", 2).head)
    .distinct()
    .collect()
    .sortBy(_.toLong)

  //File creation
  val idFile = new File(outputPath+"\\ids.txt")
  val edgeFile = new File(outputPath+"\\edges.txt")
  idFile.createNewFile() // if file already exists will do nothing
  edgeFile.createNewFile()
  val idFileWriter = new PrintWriter(idFile)
  val edgeFileWriter = new PrintWriter(edgeFile)



  var start = offsets.head
  for (offset <- offsets.tail) {
    //Bash command to copy 100 compressed pages
    val bash_command = "dd bs=1 skip=" + start + " count=" + offset.toLong.-(start.toLong) + " if=" + "\"" + dataPath + "\"" + " of=" + "\"" + tempPath + "\""
    println(bash_command.toString)
    Process(bash_command).!

    try {
      spark.sparkContext.textFile(tempPath)
        .coalesce(1)
        .saveAsTextFile(tempPathUnzip)
    } catch {
      case _ => {
        //Sometimes fails due to parralel delete... try again and pray
        spark.sparkContext.textFile(tempPath)
          .coalesce(1)
          .saveAsTextFile(tempPathUnzip)
      }
    }

    //add a dummy root node so parser works
    //https://coderwall.com/p/3ddyka/how-to-parse-a-file-xml-without-root-or-a-malformed-xml-in-java
    val file = new File(tempPathUnzip+"\\part-00000")
    val fis = new FileInputStream(file)
    val streams =
      util.Arrays.asList(
        new ByteArrayInputStream("<root>".getBytes()),
        fis,
        new ByteArrayInputStream("</root>".getBytes()))

    val cntr = new SequenceInputStream(Collections.enumeration(streams))

    //Scala XML parser from the
    val xml = new XMLEventReader(Source.fromInputStream(cntr))
    //val xml = new XMLEventReader(Source.fromFile("C:\\Users\\Eddie\\Desktop\\MASTER\\2eme\\BDA\\Zeppelin\\data\\note.xml"))

    //current page title
    var currentPage = ""
    var currentId:Long = 0

    //Store current hyperlinks in hashmap for writing later
    var links = mutable.Map.empty[String, mutable.Set[String]]
    var ids = mutable.Map.empty[Long, String]

    //Parsing flags
    var inTitle = false
    var inText = false
    var inId = false

    for (event <- xml) {
      event match {
        case EvElemStart(_, "title", _, _) => {
          inTitle = true
        }
        case EvElemEnd(_, "title") => {
          inTitle = false
        }
        case EvElemEnd(_, "text") => {
          inText = false
        }
        case EvElemStart(_, "text", _, _) => {
          inText = true
        }
        case EvElemEnd(_, "id") => {
          inId = false
        }
        case EvElemStart(_, "id", _, _) => {
          inId = true
        }

        case e @ EvText(t) => {
          if(inTitle){
            currentPage = e.text
            links += currentPage -> mutable.Set[String]()
          } else if(inText) {
            if (!currentPage.isEmpty) {
              hyperLinkRegex.r.findAllMatchIn(e.text).foreach(link => links.get(currentPage) match {
                case Some(i) => i += link.group(1)
                case _ =>
              })
            }
          } else if (inId){
            currentId = e.text.toLong
            ids += currentId -> currentPage
          }
        }
        case _ => // ignore
      }
    }

    //set new offset for new iteration
    start = offset

    //write ids and links to file before next 100 pages

    for((k,v) <- links){
      if(v.size > 0){
        edgeFileWriter.println(k + "\t" + v.reduceLeft(_ + ";;" + _))
      }
    }
    for((k,v) <- ids){
      idFileWriter.println(k+"\t"+v)
    }
    idFileWriter.flush()
    edgeFileWriter.flush()


    //clean before next 100 pages
    FileUtils.deleteQuietly(new File(tempPath))
    FileUtils.deleteDirectory(new File(tempPathUnzip))
  }

  println("Job done.")
  System.in.read();

}
