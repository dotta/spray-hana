package br.com.strategiatec.spray.hanacloud

import scala.collection.mutable.ArrayBuffer

import akka.actor.Actor
import akka.actor.Props
import akka.routing.RoundRobinRouter
import spray.json._
import spray.json.DefaultJsonProtocol._
import spray.routing.RequestContext

class MasterActor(ctx: RequestContext) extends Actor {
  import MasterActor._
  def receive: Receive = {
    case Process(lines) => context.actorOf(Props(new MapReduceActor(lines)))
    case Result(value)  => ctx.complete { value.toJson.prettyPrint }
  }
}

object MasterActor {
  sealed trait Msg 
  case class Process(lines: List[String]) extends Msg
}

class MapReduceActor(lines: List[String]) extends Actor {
  val mapActor = context.actorOf(Props[MapActor].withRouter(RoundRobinRouter(nrOfInstances = 5)), name = "map")
  val reduceActor = context.actorOf(Props[ReduceActor].withRouter(RoundRobinRouter(nrOfInstances = 5)), name = "reduce")
  val aggregateActor = context.actorOf(Props(new AggregateActor), name = "aggregate")

  var processedLines = 0

  lines foreach (mapActor ! _)

  def receive: Receive = {
    case mapData: MapData       => reduceActor ! mapData
    case reduceData: ReduceData => aggregateActor ! reduceData
    case Aggregated             =>
      processedLines += 1
      if (areAllLinesProcessed) aggregateActor ! GetResult
  }
  
  def areAllLinesProcessed: Boolean = processedLines == lines.size
}

class MapActor extends Actor {
  val STOP_WORDS = List("a", "am", "an", "and", "are", "as", "at",
    "be", "do", "go", "if", "in", "is", "it", "of", "on", "the", "to")

  def receive: Receive = {
    case message: String => sender ! countWords(message)
  }

  def countWords(line: String): MapData = MapData {
    line.split("""\s+""").foldLeft(ArrayBuffer.empty[WordCount]) {
      (index, word) =>
        if (!STOP_WORDS.contains(word.toLowerCase()))
          index += WordCount(word.toLowerCase, 1)
        else
          index
    }
  }
}

class AggregateActor extends Actor {
  val finalReducedMap = new collection.mutable.HashMap[String, Int]

  def receive: Receive = {
    case ReduceData(reduceDataMap) =>
      aggregateInMemoryReduce(reduceDataMap)
      sender ! Aggregated
    case GetResult                 =>
      sender ! Result(finalReducedMap.toMap)
  }

  def aggregateInMemoryReduce(reducedList: Map[String, Int]): Unit = {
    for ((key, value) <- reducedList) {
      if (finalReducedMap contains key)
        finalReducedMap(key) = (value + finalReducedMap.get(key).get)
      else
        finalReducedMap += (key -> value)
    }
  }
}

class ReduceActor extends Actor {
  def receive: Receive = {
    case MapData(dataList) => sender ! reduce(dataList)
  }

  def reduce(words: IndexedSeq[WordCount]): ReduceData = ReduceData {
    words.foldLeft(Map.empty[String, Int]) {
      (index, words) =>
        if (index contains words.word)
          index + (words.word -> (index.get(words.word).get + 1))
        else
          index + (words.word -> 1)
    }
  }
}

sealed trait MapReduceMessage
case class WordCount(word: String, count: Int) extends MapReduceMessage
case class MapData(dataList: ArrayBuffer[WordCount]) extends MapReduceMessage
case class ReduceData(reduceDataMap: Map[String, Int]) extends MapReduceMessage
case object Aggregated extends MapReduceMessage
case object GetResult extends MapReduceMessage
case class Result(value: Map[String, Int]) extends MapReduceMessage