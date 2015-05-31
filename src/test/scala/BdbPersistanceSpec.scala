package akka.persistence.journal.bdb

import akka.actor._
import akka.testkit._
import akka.persistence._
import akka.persistence.journal.JournalSpec
import akka.persistence.snapshot.SnapshotStoreSpec

import com.typesafe.config.ConfigFactory

import org.apache.commons.io.FileUtils

import org.scalatest._

import scala.concurrent.duration._

import java.io.File


class BdbSnapshotStoreSpec extends SnapshotStoreSpec {
  lazy val config = ConfigFactory.parseString(
    """
      |akka.persistence.journal.plugin = "bdb-journal"
      |akka.persistence.snapshot-store.plugin = "bdb-snapshot-store"
      |akka.persistence.publish-plugin-commands = on
      |akka.persistence.publish-confirmations = on
      |bdb-persistence.dir = "target/replication"
    """.stripMargin)

  protected override def afterAll(): Unit = {
    FileUtils.deleteDirectory(new File(config.getString("bdb-persistence.dir")))
    super.afterAll()
  }
}


object BdbJournalSpec {
  case class Delete(snr: Long, permanent: Boolean)

  class SlowDestination(probe: ActorRef, maxReceived: Long) extends Actor {
    import context.dispatcher

    val delay = 100.millis
    var received = Vector.empty[ConfirmablePersistent]


    def receive = {
      case cp: ConfirmablePersistent ⇒ {
        if (received.isEmpty) context.system.scheduler.scheduleOnce(delay, self, "confirm")
        received :+= cp
      }

      case "confirm" ⇒ {
        if (received.size > maxReceived) {
          probe ! s"number of received messages to high: ${received.size}"
        } else {
          probe ! received.head.payload
        }

        received.head.confirm()
        received = received.tail
        if (received.nonEmpty) context.system.scheduler.scheduleOnce(delay, self, "confirm")
      }
    }
  }
}


class BdbJournalSpec
  extends WordSpecLike
  with JournalSpec
  with ImplicitSender
  with Matchers
{
  import BdbJournalSpec._

  lazy val config = ConfigFactory.parseString(
    """
      |akka.persistence.journal.plugin = "bdb-journal"
      |akka.persistence.snapshot-store.local.dir = "target/snapshots"
      |akka.persistence.publish-plugin-commands = on
      |akka.persistence.publish-confirmations = on
      |bdb-persistence.dir = "target/replication"
    """.stripMargin)


  "A BDB journal" should {
    "not flood persistent channels" in {
      val probe = TestProbe()

      val settings = PersistentChannelSettings(
        redeliverMax = 0,
        redeliverInterval = 1.minute,
        pendingConfirmationsMax = 4,
        pendingConfirmationsMin = 2)

      val channel = system actorOf {
        PersistentChannel.props(s"test1-watermark", settings)
      }

      val destination = system actorOf {
        Props(classOf[SlowDestination], probe.ref, settings.pendingConfirmationsMax)
      }

      1 to 10 foreach { i ⇒ channel ! Deliver(Persistent(i), destination.path) }
      1 to 10 foreach { i ⇒ probe.expectMsg(i) }
      system.stop(channel)
    }
  }


  protected override def afterAll(): Unit = {
    FileUtils.deleteDirectory(new File(config.getString("akka.persistence.snapshot-store.local.dir")))
    FileUtils.deleteDirectory(new File(config.getString("bdb-persistence.dir")))
    super.afterAll()
  }
}

