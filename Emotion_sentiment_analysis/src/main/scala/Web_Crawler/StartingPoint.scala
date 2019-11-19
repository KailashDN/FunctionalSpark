package me.foat.crawler

import akka.actor.{ActorSystem, PoisonPill, Props}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps
/**
 * The App class of the project should do several things, such as:
 * creating the main Actor and initializing it with a first website we want to process,
 * stopping the whole system if the process was not ended yet.
 */
object StartingPoint extends App {
  val system = ActorSystem()
  val supervisor = system.actorOf(Props(new Supervisor(system)))

  supervisor ! Start("https://www.amazon.com/Apple-Watch-GPS-44mm-Space-Aluminium/dp/B07K3HLMTF/ref=sxin_2_ac_d_rm?ac_md=0-0-YXBwbGUgd2F0Y2g%3D-ac_d_rm&crid=GLEDN8A85GKY&keywords=apple%2Bwatch&pd_rd_i=B07K3HLMTF&pd_rd_r=f33ab626-7da8-48a4-a130-2160ce18a568&pd_rd_w=4enbN&pd_rd_wg=AaZXo&pf_rd_p=e2f20af2-9651-42af-9a45-89425d5bae34&pf_rd_r=TKJ8KZF21F0YA7CQX7Q2&qid=1574135655&sprefix=watcch%2Caps%2C156&th=1")
  //"https://foat.me"
  //https://www.amazon.com/Apple-Watch-GPS-44mm-Space-Aluminium/dp/B07K3HLMTF/ref=sxin_2_ac_d_rm?ac_md=0-0-YXBwbGUgd2F0Y2g%3D-ac_d_rm&crid=GLEDN8A85GKY&keywords=apple%2Bwatch&pd_rd_i=B07K3HLMTF&pd_rd_r=f33ab626-7da8-48a4-a130-2160ce18a568&pd_rd_w=4enbN&pd_rd_wg=AaZXo&pf_rd_p=e2f20af2-9651-42af-9a45-89425d5bae34&pf_rd_r=TKJ8KZF21F0YA7CQX7Q2&qid=1574135655&sprefix=watcch%2Caps%2C156&th=1

  Await.result(system.whenTerminated, 10 minutes)

  supervisor ! PoisonPill
  system.terminate
}
