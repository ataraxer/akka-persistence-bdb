package akka.persistence.journal.bdb

import org.scalatest.{Suite, BeforeAndAfterAll}
import akka.testkit.TestKit
import org.apache.commons.io.FileUtils
import java.io.File

trait BdbCleanup extends BeforeAndAfterAll {
  this: TestKit with Suite =>

  val config = system.settings.config.getConfig("bdb-journal")
  val snapshotConfig = system.settings.config.getConfig("akka.persistence.snapshot-store.local")

  override protected def afterAll(): Unit = {
    FileUtils.deleteDirectory(new File(snapshotConfig.getString("dir")))
    FileUtils.deleteDirectory(new File(config.getString("dir")))

    system.shutdown()
    system.awaitTermination()
  }

}
