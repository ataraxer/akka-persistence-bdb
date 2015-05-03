package akka.persistence.journal.bdb

import java.io.File
import java.nio.ByteBuffer

import akka.actor.Actor
import akka.persistence.journal.SyncWriteJournal
import akka.persistence.{PersistentConfirmation, PersistentId, PersistentRepr}
import akka.serialization.SerializationExtension

import com.sleepycat.je._

import scala.annotation.tailrec
import scala.collection.immutable.Seq


trait BdbEnvironment extends Actor {
  private[bdb] val config = context.system.settings.config.getConfig("bdb-journal")


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


class BdbJournal
  extends SyncWriteJournal
  with BdbEnvironment
  with BdbKeys
  with BdbReplay
{
  import BdbClient._

  val serialization = SerializationExtension(context.system)


  private[bdb] implicit val txConfig = {
    new TransactionConfig()
    .setDurability(
      if (config.getBoolean("sync")) Durability.COMMIT_SYNC
      else Durability.COMMIT_WRITE_NO_SYNC)
    .setReadCommitted(true)
  }


  private[bdb] val db = {
    val dbConfig = {
      new DatabaseConfig()
      .setAllowCreate(true)
      .setTransactional(true)
      .setSortedDuplicates(true)
    }

    env.openDatabase(NoTransaction, "journal", dbConfig)
  }


  override def postStop(): Unit = {
    db.close()
    super.postStop()
  }


  private[bdb] def bdbSerialize(persistent: PersistentRepr): DatabaseEntry = {
    val payload = serialization.serialize(persistent).get
    val buffer = ByteBuffer.allocate(payload.size + 1)
    buffer.put(DataMagicByte)
    buffer.put(payload)
    new DatabaseEntry(buffer.array)
  }


  def writeMessages(messages: Seq[PersistentRepr]): Unit = {
    var max = Map.empty[Long, Long].withDefaultValue(-1L)

    db withTransaction { implicit tx =>
      messages foreach { m =>
        val pid = getPersistenceId(m.persistenceId)

        val operation = db.putKey(getKey(pid, m.sequenceNr), bdbSerialize(m))

        if (operation.hasFailed) {
          throw new IllegalStateException("Failed to write message to database")
        }

        if (max(pid) < m.sequenceNr) {
          max += (pid -> m.sequenceNr)
        }
      }

      for ((p, m) <- max) {
        val key = getMaxSeqnoKey(p)
        db.deleteKey(key)
        val entry = new DatabaseEntry(ByteBuffer.allocate(8).putLong(m).array)

        if (db.putKey(key, entry).hasFailed) {
          throw new IllegalStateException("Failed to write maxSeqno entry to database.")
        }
      }
    }
  }


  def writeConfirmations(confirmations: Seq[PersistentConfirmation]): Unit = {
    db withTransaction { implicit tx =>
      confirmations foreach { c =>
        val cid = c.channelId.getBytes("UTF-8")

        val entry = new DatabaseEntry(
          ByteBuffer
          .allocate(cid.size + 1)
          .put(ConfirmMagicByte)
          .put(cid)
          .array)

        val operation = db.putKey(getKey(c.persistenceId, c.sequenceNr), entry)

        if (operation.hasFailed) {
          throw new IllegalStateException("Failed to write confirmation to database.")
        }
      }
    }
  }


  def deleteMessages(messageIds: Seq[PersistentId], permanent: Boolean): Unit = {
    db withTransaction { implicit tx =>
      messageIds foreach { m =>
        db.deleteKey(getKey(m.persistenceId, m.sequenceNr), permanent)
      }
    }
  }


  private[bdb] def keyRangeCheck(
    entry: DatabaseEntry,
    persistenceId: Long,
    minSeqno: Long,
    maxSeqno: Long): Boolean =
  {
    val buf = ByteBuffer.wrap(entry.getData)
    val pid = buf.getLong
    val sno = buf.getLong
    persistenceId == pid && sno >= minSeqno && sno <= maxSeqno
  }


  def deleteMessagesTo(
    persistenceId: String,
    toSequenceNr: Long,
    permanent: Boolean): Unit =
  {
    @tailrec
    def iterateCursor(cursor: Cursor, persistenceId: Long): Unit = {
      val BdbSuccess((dbKey, dbVal)) = cursor.getCurrentKey(LockMode.DEFAULT)

      if (keyRangeCheck(dbKey, persistenceId, 1L, toSequenceNr)) {
        cursor.deleteKey(dbKey, permanent)
        if (cursor.getNextNoDup(dbKey, dbVal, LockMode.DEFAULT) == OperationStatus.SUCCESS)
          iterateCursor(cursor, persistenceId)
      }
    }

    db withTransactionalCursor { cursor =>
      val operationStatus = {
        cursor.getSearchKeyRange(
        getKey(persistenceId, 1L),
        new DatabaseEntry,
        LockMode.DEFAULT)
      }

      if (operationStatus == OperationStatus.SUCCESS) {
        iterateCursor(cursor, getPersistenceId(persistenceId))
      }
    }
  }
}

