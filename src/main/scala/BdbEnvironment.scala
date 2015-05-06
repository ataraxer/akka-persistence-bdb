package akka.persistence.journal.bdb

import java.io.File
import java.nio.ByteBuffer

import akka.actor.Actor
import akka.persistence.journal.SyncWriteJournal
import akka.persistence.{PersistentConfirmation, PersistentId, PersistentRepr}
import akka.serialization.SerializationExtension

import com.sleepycat.je._
import com.sleepycat.je.rep._

import scala.collection.JavaConversions._

import java.util.concurrent.TimeUnit


abstract class BdbEnvironment(configPath: String) extends Actor {
  private[bdb] val config = context.system.settings.config.getConfig(configPath)


  private[bdb] val env: Environment = {
    import EnvironmentConfig._

    val journalDir = new File(config.getString("dir"))

    journalDir.mkdirs()


    val envConfig = {
      val result = new EnvironmentConfig

      result.setTransactional(true)

      val durability = new Durability(
        Durability.SyncPolicy.SYNC,
        Durability.SyncPolicy.SYNC,
        Durability.ReplicaAckPolicy.ALL)

      result.setDurability(durability)

      result.setAllowCreate(true)
      //result.setLocking(true)
      val logLevel = config.getString("loglevel").toUpperCase
      result.setConfigParam(EnvironmentConfig.CONSOLE_LOGGING_LEVEL, logLevel)

      result.setConfigParam(CLEANER_THREADS, config.getString("cleaner-threads"))
      result.setConfigParam(ENV_DUP_CONVERT_PRELOAD_ALL, "false")
      result.setConfigParam(LOG_GROUP_COMMIT_INTERVAL, config.getString("group-commit-interval"))
      result.setConfigParam(STATS_COLLECT, config.getString("stats-collect"))
      result.setConfigParam(MAX_MEMORY_PERCENT, config.getString("cache-size-percent"))

      result
    }


    if (config.getBoolean("replication.enabled")) {
      val replicationConfig = {
        val result = new ReplicationConfig

        /* Set consistency policy for replica. */
        val consistencyPolicy = new TimeConsistencyPolicy(
          1, TimeUnit.SECONDS, /* 1 sec of lag */
          3, TimeUnit.SECONDS  /* Wait up to 3 sec */);

        result.setConsistencyPolicy(consistencyPolicy);

        /* Wait up to two seconds for commit acknowledgments. */
        result.setReplicaAckTimeout(2, TimeUnit.SECONDS);

        result.setGroupName(config.getString("replication.group-name"))
        result.setNodeName(config.getString("replication.node-name"))
        result.setNodeHostPort(config.getString("replication.address"))

        val helpers = config.getStringList("replication.helper-hosts")

        result.setHelperHosts(helpers.mkString(","))

        result
      }


      new ReplicatedEnvironment(journalDir, replicationConfig, envConfig)
    } else {
      new Environment(journalDir, envConfig)
    }
  }


  override def postStop(): Unit = {
    env.close()
    super.postStop()
  }
}

