package akka.persistence.journal.bdb

import java.io.File
import java.nio.ByteBuffer

import akka.actor.Actor
import akka.persistence.journal.SyncWriteJournal
import akka.persistence.{PersistentConfirmation, PersistentId, PersistentRepr}
import akka.serialization.SerializationExtension

import com.sleepycat.je._


abstract class BdbEnvironment(configPath: String) extends Actor {
  private[bdb] val config = context.system.settings.config.getConfig(configPath)


  private[bdb] val env = {
    import EnvironmentConfig._

    val journalDir = new File(config.getString("dir"))

    journalDir.mkdirs()

    val envConfig = {
      new EnvironmentConfig()
      .setAllowCreate(true)
      .setTransactional(true)
      .setLocking(true)
      .setConfigParam(CLEANER_THREADS, config.getString("cleaner-threads"))
      .setConfigParam(ENV_DUP_CONVERT_PRELOAD_ALL, "false")
      .setConfigParam(LOG_GROUP_COMMIT_INTERVAL, config.getString("group-commit-interval"))
      .setConfigParam(STATS_COLLECT, config.getString("stats-collect"))
      .setConfigParam(MAX_MEMORY_PERCENT, config.getString("cache-size-percent"))
    }

    new Environment(journalDir, envConfig)
  }


  override def postStop(): Unit = {
    env.close()
    super.postStop()
  }
}


