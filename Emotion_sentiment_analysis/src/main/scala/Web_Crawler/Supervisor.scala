package me.foat.crawler
import java.net.URL
import akka.actor.{Actor, ActorSystem, Props, _}
import scala.language.postfixOps

/**
 *
 * @param system
 * The Supervisor has four basic variables:
 *
 * How many pages we visited (sent for scraping).
 * Which pages we still need to scrap.
 * How many times we tried to visit a particular page. This is needed, since we think that scraping might fail and in that case we need to visit a page several times.
 * Store for SiteCrawler Actors for each host we need to deal with.
 */

class Supervisor(system: ActorSystem) extends Actor {
  val indexer = context actorOf Props(new Indexer(self))

  val maxPages = 100
  val maxRetries = 2
  /**
   * When we scrap a url, we need to send it to an Actor which processes urls for one particular host.
   */
  var numVisited = 0
  var toScrap = Set.empty[URL]
  var scrapCounts = Map.empty[URL, Int]
  var host2Actor = Map.empty[String, ActorRef]

  /**
   * The receive function body of the Supervisor class contains a handler for each received message.
   * Each message is a case class. To see all messages, check the Messages class in the project repository.
   */
  def receive: Receive = {
    case Start(url) =>
      println(s"starting $url")
      scrap(url)
    case ScrapFinished(url) =>
      println(s"scraping finished $url")

    /**
     * When the Indexer finishes its processing, it sends the url information to the Supervisor using IndexFinished message.
     */
    case IndexFinished(url, urls) =>
      if (numVisited < maxPages)
        urls.toSet.filter(l => !scrapCounts.contains(l)).foreach(scrap)
      checkAndShutdown(url)
    case ScrapFailure(url, reason) =>
      val retries: Int = scrapCounts(url)
      println(s"scraping failed $url, $retries, reason = $reason")
      if (retries < maxRetries) {
        countVisits(url)
        host2Actor(url.getHost) ! Scrap(url)
      } else
        checkAndShutdown(url)
  }

  /**
   * checkAndShutdown function removes url from toScrap set and, if there is no urls to visit anymore — shutdowns the whole system.
   */
  def checkAndShutdown(url: URL): Unit = {
    toScrap -= url
    // if nothing to visit
    if (toScrap.isEmpty) {
      self ! PoisonPill
      system.terminate()
    }
  }

  /**
   * Here, we check if an Actor for the provided host is presented, if not, we create it and add to our host2Actor container.
   * Next, we add the url to the collection of pages we want to scrap.
   * After all, we notify the SiteCrawler Actor that we want to scrap the url.
   */
  def scrap(url: URL) = {
    val host = url.getHost
    println(s"host = $host")
    if (!host.isEmpty) {
      val actor = host2Actor.getOrElse(host, {
        val buff = system.actorOf(Props(new SiteCrawler(self, indexer)))
        host2Actor += (host -> buff)
        buff
      })

      numVisited += 1
      toScrap += url
      countVisits(url)
      actor ! Scrap(url)
    }
  }

  def countVisits(url: URL): Unit = scrapCounts += (url -> (scrapCounts.getOrElse(url, 0) + 1))
}