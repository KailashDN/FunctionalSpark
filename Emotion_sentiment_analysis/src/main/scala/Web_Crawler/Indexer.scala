package me.foat.crawler
import java.net.URL
import akka.actor.{Actor, ActorRef}

/**
 * The two main goals of the Indexer Actor is to store the sent information
 * and to send all scraped urls to the Supervisor. To make things more fun,
 * we print all received data before the Actor stops working.
 * @param supervisor
 */
class Indexer(supervisor: ActorRef) extends Actor {
  var store = Map.empty[URL, Content]

  def receive: Receive = {
    case Index(url, content) =>
      println(s"saving page $url with $content")
      store += (url -> content)
      supervisor ! IndexFinished(url, content.urls)
  }

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    super.postStop()
    store.foreach(println)
    println(store.size)
  }
}