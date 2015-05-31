package akka.persistence.journal.bdb

import java.io.File
import java.nio.ByteBuffer

import akka.actor.Actor
import akka.persistence.journal.SyncWriteJournal
import akka.persistence.{PersistentConfirmation, PersistentId, PersistentRepr}
import akka.serialization.SerializationExtension

import com.sleepycat.je._
import com.sleepycat.je.rep._

import com.typesafe.config.Config

import scala.collection.JavaConversions._

import java.util.concurrent.TimeUnit


object BdbEnvironment {
  def apply(config: Config): BdbEnvironment = new BdbEnvironment(config)
}


final class BdbEnvironment(config: Config) {
  val isReplicated = config.getBoolean("replication.enabled")


  private[bdb] val env: Environment = {
    import EnvironmentConfig._

    val directory = new File(config.getString("dir"))

    directory.mkdirs()


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


    if (isReplicated) {
      val replicationConfig = {
        val result = new ReplicationConfig

        /* Set consistency policy for replica. */
        val consistencyPolicy = new TimeConsistencyPolicy(
          1, TimeUnit.SECONDS, /* 1 sec of lag */
          3, TimeUnit.SECONDS  /* Wait up to 3 sec */)

        result.setConsistencyPolicy(consistencyPolicy)

        /* Wait up to two seconds for commit acknowledgments. */
        result.setReplicaAckTimeout(2, TimeUnit.SECONDS)

        result.setGroupName(config.getString("replication.group-name"))
        result.setNodeName(config.getString("replication.node-name"))
        result.setNodeHostPort(config.getString("replication.address"))

        val helpers = config.getStringList("replication.helper-hosts")

        result.setHelperHosts(helpers.mkString(","))

        result
      }

      new ReplicatedEnvironment(directory, replicationConfig, envConfig)
    } else {
      new Environment(directory, envConfig)
    }
  }


  def openDatabase(name: String) = {
    val dbConfig = {
      new DatabaseConfig()
      .setAllowCreate(isMaster)
      .setTransactional(true)
      .setSortedDuplicates(true)
    }

    env.openDatabase(BdbClient.NoTransaction, name, dbConfig)
  }


  def isMaster = {
    env match {
      case replicatedEnv: ReplicatedEnvironment => replicatedEnv.getState.isMaster
      case _ => true
    }
  }


  def shutdown(): Unit = {
    env.close()
  }
}

